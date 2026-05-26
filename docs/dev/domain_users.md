# Domain User Support

## Overview

The worker agent supports Active Directory domain users for both the **agent user** (the identity the agent service runs as) and the **queue user** (the identity jobs run as). The agent does not create domain users — it uses whatever the user provides.

## User Formats

Windows supports two formats for specifying domain users:

- **Down-Level Logon Name (DDL)**: `DOMAIN\username`
- **User Principal Name (UPN)**: `username@domain.com`

Both formats must be accepted anywhere a Windows username is specified.

## Agent User (Service Identity)

The installer accepts a pre-existing domain user as the agent user. It will:

- **Not** create the user (unlike local users where it calls `NetUserAdd`)
- **Not** reset the user's password (domain password management is external)
- Validate that the provided credentials can log on
- Grant required user rights (SeServiceLogonRight, etc.) if `--grant-required-access` is specified
- Add the user to the local Administrators group if needed

The user must already exist in Active Directory before running the installer.

### Prerequisites for the Agent Domain User

The domain user account must have (or the installer will grant with `--grant-required-access`):

- **Local Administrators group membership** — required for `LoadUserProfileW` and `CreateProcessAsUserW`
- **SeServiceLogonRight** — log on as a service
- **SeIncreaseQuotaPrivilege** — adjust memory quotas for a process
- **SeAssignPrimaryTokenPrivilege** — replace a process-level token

Additionally, the user must be a member of the local `deadline-job-users` group (the installer handles this with `--grant-required-access`).

Without `--grant-required-access`, the installer will error out and tell you which permissions are missing so they can be pre-configured via Group Policy.

**Password:** Provided at install time via `--password` flag or interactive prompt. Windows stores it in the Service Control Manager (SCM) as part of the service configuration — the agent never sees it again after install. If the AD password is rotated, the service must be reconfigured with the new password (via `sc.exe config` or re-running the installer).

## Queue User (Job Identity)

The queue user flow is **the same for both local and domain users**:

1. The Deadline service provides a `user` and `passwordArn` (configured on the queue's `jobRunAsUser.windows`)
2. The agent fetches the password from Secrets Manager via `GetSecretValue`
3. The agent calls `LogonUser` with the credentials and caches the logon token

**Password:** Stored in AWS Secrets Manager as `{"password": "value"}`. The AD admin sets/rotates the password in AD, and keeps the Secrets Manager secret in sync (manually or via automation). The agent re-fetches on cache miss or credential failure.

The only difference: local queue users can fall back to `reset_user_password()` (via `NetUserSetInfo`) if no `passwordArn` is configured. Domain users cannot — they always require a Secrets Manager secret.

## Username Format Handling

Different Win32 APIs accept different username formats. Rather than normalizing to a single format on input, we keep the username in whatever format the user provided and handle each format at the relevant call site.

### Utility: `windows/win_user.py`

```python
is_domain_user(username)        # True if contains '\' or '@'
```

Domain parsing and format conversion are done inline at each call site using `TranslateName` (for DDL conversion) and string splitting (for `LogonUser` domain parameter).

### API Compatibility by Format

| API | Local (`user`) | DDL (`DOMAIN\user`) | UPN (`user@domain`) |
|-----|:-:|:-:|:-:|
| `LookupAccountName(None, name)` | ✅ | ✅ | ✅ |
| `LogonUser(user, domain, ...)` | ✅ | ✅ (split first) | ✅ (domain=None) |
| `LoadUserProfile(token, {UserName: ...})` | ✅ | ✅ (strips domain internally) | ❌ (strip `@domain` first) |
| `TranslateName(name, from, to)` | ✅ | ✅ | ✅ |
| `NetUserGetLocalGroups(None, user)` | ✅ | ✅ | ❌ (use `TranslateName` to DDL first) |
| `NetUserGetInfo(None, user, ...)` | ✅ | ❌ | ❌ |
| `NetUserSetInfo(None, user, ...)` | ✅ | ❌ | ❌ |
| `NetLocalGroupAddMembers` (`domainandname`) | ✅ | ✅ | ❌ |
| `CreateProcessWithLogonW(user, domain, ...)` | ✅ | ✅ (split first) | ✅ (domain=None) |

### Strategy

1. Accept any format from the user (DDL or UPN) and preserve it as-is.
2. At call sites that need domain and user as separate args (`LogonUser`, `CreateProcessWithLogonW`): split DDL on `\`, pass UPN with `domain=None`.
3. At call sites that require DDL (`NetUserGetLocalGroups`): use `TranslateName(user, win32con.NameUserPrincipal, win32con.NameSamCompatible)`.
4. At call sites that use the name as a folder (`LoadUserProfile`): strip `@domain` from UPN to get bare username.
5. For identity comparison (`is_process_user`, `users_equal`): compare by SID, not string.
6. For `NetUser*` APIs (local-only): skip these for domain users — they only apply to local accounts.

## LogonUser Domain Parameter

The Win32 `LogonUser` / `LogonUserW` API accepts a separate `domain` parameter:

- For DDL (`DOMAIN\user`): pass `domain="DOMAIN"`, `username="user"`
- For UPN (`user@domain.com`): pass `domain=None`, `username="user@domain.com"` (Windows resolves UPN automatically when domain is NULL)
- For local users: pass `domain=None` or `domain="."`, `username="user"`

Reference: [LogonUserW docs](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-logonuserw) — "If you use the user principal name (UPN) format, User@DNSDomainName, the lpszDomain parameter must be NULL."

## LoadUserProfile and Username Format

`LoadUserProfile` uses `lpUserName` as the **base name of the profile directory** (e.g. `C:\Users\<lpUserName>`). It does not parse domain formats:

- DDL (`DOMAIN\user`): works on modern Windows (strips domain internally)
- UPN (`user@domain.com`): **does not work** — creates a folder named `user@domain.com`
- Bare username (`user`): works

Since the Windows SCM resolves UPN to DDL when starting a service, the profile must be created with a name that matches what the SCM will look for. Always strip the `@domain` suffix before passing to `LoadUserProfile`.

Reference: [Microsoft KB](https://support.microsoft.com/en-us/topic/01e698e9-b945-56ad-3c9e-6a3edbc99f81) — "the domain\user format is not a valid format for the folder name"

## TranslateName

`TranslateName` converts between username formats (UPN ↔ DDL). The `EXTENDED_NAME_FORMAT` constants are in `win32con`, **not** `win32security`:

```python
import win32con
import win32security

ddl = win32security.TranslateName(upn, win32con.NameUserPrincipal, win32con.NameSamCompatible)
```

Reference: [TranslateName docs](https://learn.microsoft.com/en-us/windows/win32/api/secext/nf-secext-translatenamew)

## Local Permissions and `--grant-required-access`

The installer modifies the domain user's **local machine** permissions — it never touches the AD account itself.

What it does locally:
- Grants user rights (`SeServiceLogonRight`, `SE_INCREASE_QUOTA_NAME`, `SE_ASSIGNPRIMARYTOKEN_NAME`) via `LsaAddAccountRights`
- Adds the user to the local `Administrators` group (required for `LoadUserProfileW` and `CreateProcessAsUserW`)

This is **opt-in** via `--grant-required-access`. Without the flag, the installer errors out if the user exists but lacks the required permissions. This is intentional — modifying an existing account's privileges should be a deliberate choice.

### Enterprise Alternative: Group Policy

In managed environments, IT can pre-configure these permissions centrally via AD Group Policy instead of using `--grant-required-access`:

- **Local Administrators membership** — GPO → Restricted Groups or Group Policy Preferences → add the domain user to the local `Administrators` group on targeted machines
- **User rights** — GPO → Computer Configuration → Windows Settings → Security Settings → Local Policies → User Rights Assignment

When permissions are pre-configured via GPO, the installer detects them and proceeds without requiring `--grant-required-access`.


## Password Management

| User Type | Password Reset | Password Source |
|-----------|---------------|-----------------|
| Local user (job override) | `NetUserSetInfo` (default) | Agent generates and resets |
| Local user (job override + password ARN) | **Not reset** — fetched from Secrets Manager | Secrets Manager (`windows_job_user_password_arn`) |
| Local user (queue configured) | `NetUserSetInfo` via credentials resolver | Secrets Manager |
| Domain user (job override) | **Not supported** — must provide password externally | Secrets Manager (`windows_job_user_password_arn`) |
| Domain user (queue configured) | **Not supported** — managed in AD | Secrets Manager |

## Testing

### E2E Tests (`test/e2e/test_domain_user.py`)

The e2e tests deploy a Windows Server EC2 instance and set it up as a domain controller:

1. **Promote to DC**: `Install-ADDSForest` via SSM (triggers reboot)
2. **Wait for SSM**: Poll `describe_instance_information` until `PingStatus == "Online"`
3. **Create domain users**: `New-ADUser` for both agent and job users
4. **Grant user rights**: `secedit` to grant `SeServiceLogonRight` etc. to the agent user
5. **Reinstall worker agent**: `install-deadline-worker --user TEST\domain-agent --password ...` (DDL) and `install-deadline-worker --user domain-agent@test.local --password ...` (UPN)
6. **Submit jobs**: Verify `whoami` output matches the domain job user

Tests cover:
- Agent service running as a domain user
- Job running as domain user specified in DDL format (`TEST\domain-job-user`)
- Job running as domain user specified in UPN format (`domain-job-user@test.local`)

### Running Domain User Tests

```bash
# Requires Windows e2e infrastructure + OPERATING_SYSTEM=windows
export OPERATING_SYSTEM=windows
hatch run e2e:test test/e2e/test_domain_user.py
```

## `is_process_user()` — SID Comparison

The `SessionUser.is_process_user()` method determines whether the session user is the same as the process owner. This is used to decide whether impersonation is needed.

Previously this was a string comparison (`self.user == self._get_process_user()`), which fails when the same account is specified in different formats (e.g. UPN vs DDL). The fix uses `LookupAccountName` to resolve both to SIDs and compares those instead.

- The process user's SID is cached via `@lru_cache(maxsize=1)` since it never changes.
- Uses `sys.platform == "win32"` guard so mypy understands the conditional imports.
- Falls back to string comparison if `LookupAccountName` fails (e.g. account not yet resolvable).

Reference: [LookupAccountName docs](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-lookupaccountnamew)

## Future Work: Kerberos S4U (Passwordless Domain User Logon)

An alternative to the Secrets Manager password approach is Kerberos Service-for-User (S4U) delegation, which would allow the agent to obtain logon tokens for domain job users **without passwords**.

### How It Would Work

1. Agent service account is a domain user configured for **constrained delegation** in AD.
2. Instead of `LogonUserW` with a password, use:
   - `LsaRegisterLogonProcess` → `LsaLookupAuthenticationPackage` (Kerberos) → `LsaLogonUser` with `KERB_S4U_LOGON`
3. Resulting token used with `CreateProcessAsUserW` as normal.

### Prerequisites

- Agent service account must be a **domain account**
- `SeTcbPrivilege` ("Act as part of the operating system") on the host
- **Constrained delegation** configured in AD for the agent account
- Target job users must not be marked "Account is sensitive and cannot be delegated"

### Why Deferred

- Secrets Manager approach works universally without AD admin configuration
- `SeTcbPrivilege` is extremely powerful (SYSTEM-level)
- Constrained delegation requires deliberate AD topology decisions per customer
- Current approach is simpler to set up and debug

### When to Revisit

- Customers request passwordless domain user support
- Environments with existing Kerberos delegation infrastructure
- Policies against storing passwords externally
