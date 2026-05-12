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

## Queue User (Job Identity)

For queue-configured users, domain users are resolved via the existing Secrets Manager flow:

1. The Deadline service provides a `user` and `passwordArn`
2. The agent fetches the password from Secrets Manager
3. The agent calls `LogonUser` with the domain user credentials

Key difference from local users: **password reset is not supported for domain users**. The `reset_user_password()` path (which uses `NetUserSetInfo`) only works for local users. For domain users, the password must be managed externally (via Secrets Manager rotation or AD administration).

## Changes Required

### openjd-sessions

| File | Change | Status |
|------|--------|--------|
| `_win32/_helpers.py` | `logon_user()` parses domain from username and passes it to `LogonUserW` | ✅ Done |
| `_win32/_popen_as_user.py` | `CreateProcessWithLogonW` parses domain from `self.user.user` | ✅ Done |
| `_session_user.py` | Already handles `DOMAIN\user` in `_validate_username_password` | No change needed |

### worker-agent

| File | Change | Status |
|------|--------|--------|
| `windows/win_user_util.py` | New module: `is_domain_user()`, `parse_domain_and_user()`, `resolve_to_ddl()` | ✅ Done |
| `windows/win_logon.py` | `get_windows_credentials()` parses domain for `LogonUser`. `reset_user_password()` raises for domain users. | ✅ Done |
| `windows/win_credentials_resolver.py` | Works as-is (password comes from Secrets Manager) | No change needed |
| `installer/win_installer.py` | Accepts domain user as agent user. Skips user creation. Parses domain in `ensure_user_profile_exists`. | ✅ Done |
| `config/config.py` | Domain job user override requires `windows_job_user_password_arn`. Defers credential resolution to runtime. | ✅ Done |
| `test/e2e/test_domain_user.py` | E2E tests: promotes instance to DC, creates domain users, verifies jobs run as domain user (DDL + UPN) | ✅ Done |

## Username Normalization

Different Win32 APIs accept different username formats. Rather than handling each format at every call site, we normalize usernames to DDL (`DOMAIN\user`) on input using `win_user_util.resolve_to_ddl()`.

### Utility: `windows/win_user_util.py`

```python
is_domain_user(username)        # True if contains '\' or '@'
parse_domain_and_user(username) # Split into (domain, user) for LogonUser
resolve_to_ddl(username)        # Convert any format -> "DOMAIN\user" via SID lookup
```

### API Compatibility by Format

| API | Local (`user`) | DDL (`DOMAIN\user`) | UPN (`user@domain`) |
|-----|:-:|:-:|:-:|
| `LookupAccountName(None, name)` | ✅ | ✅ | ✅ |
| `LogonUser(user, domain, ...)` | ✅ | ✅ (split first) | ✅ (domain=None) |
| `NetUserGetInfo(None, user, ...)` | ✅ | ❌ | ❌ |
| `NetUserSetInfo(None, user, ...)` | ✅ | ❌ | ❌ |
| `NetLocalGroupAddMembers` (`domainandname`) | ✅ | ✅ | ❌ |
| `CreateProcessWithLogonW(user, domain, ...)` | ✅ | ✅ (split first) | ✅ (domain=None) |

### Strategy

1. Accept any format from the user (DDL or UPN).
2. On input, call `resolve_to_ddl()` to normalize to `DOMAIN\user`.
3. Use `parse_domain_and_user()` when calling `LogonUser` / `CreateProcessWithLogonW` (these need domain and user as separate args).
4. For `NetUser*` APIs (local-only): skip these for domain users — they only apply to local accounts.

## LogonUser Domain Parameter

The Win32 `LogonUser` / `LogonUserW` API accepts a separate `domain` parameter:

- For DDL (`DOMAIN\user`): pass `domain="DOMAIN"`, `username="user"`
- For UPN (`user@domain.com`): pass `domain=None`, `username="user@domain.com"` (Windows resolves UPN automatically when domain is NULL)
- For local users: pass `domain=None` or `domain="."`, `username="user"`

## Password Management

| User Type | Password Reset | Password Source |
|-----------|---------------|-----------------|
| Local user (job override) | `NetUserSetInfo` (existing behavior) | Agent generates and resets |
| Local user (queue configured) | `NetUserSetInfo` via credentials resolver | Secrets Manager |
| Domain user (job override) | **Not supported** — must provide password externally | TBD |
| Domain user (queue configured) | **Not supported** — managed in AD | Secrets Manager |

## Testing

### E2E Tests (`test/e2e/test_domain_user.py`)

The e2e tests deploy a Windows Server EC2 instance and set it up as a domain controller:

1. **Promote to DC**: `Install-ADDSForest` via SSM (triggers reboot)
2. **Wait for SSM**: Poll `describe_instance_information` until `PingStatus == "Online"`
3. **Create domain users**: `New-ADUser` for both agent and job users
4. **Grant user rights**: `secedit` to grant `SeServiceLogonRight` etc. to the agent user
5. **Reinstall worker agent**: `install-deadline-worker --user TEST\domain-agent --password ...`
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

### Unit Tests

The `is_domain_user()` and `parse_domain_and_user()` utilities should have unit tests covering:
- Local user: `"job-user"` → not domain, `(None, "job-user")`
- DDL: `"DOMAIN\\user"` → domain, `("DOMAIN", "user")`
- UPN: `"user@domain.com"` → domain, `(None, "user@domain.com")`
- `reset_user_password()` raises `PasswordResetException` for domain users
