# E2E Test Strategy: Moving Away from CloudWatch Log Assertions

## Problem

Many e2e tests use `job.assert_single_task_log_contains()` to validate test outcomes by polling CloudWatch Logs for expected patterns. This causes:

1. **Flakiness** — CloudWatch Logs is eventually consistent; logs may not appear within the polling window.
2. **Slow tests** — Polling loops (often with backoff up to 120s) add significant duration.
3. **Unnecessary coupling** — Most tests don't need to verify CW log delivery; they just need to know if the job produced the right output.

## Decision

- **Prefer job result + job attachments** for asserting test outcomes (like `test_domain_user.py` does).
- **Only use CloudWatch log assertions** when testing CW-specific behaviour (e.g., "logs were uploaded").

## Investigation: Tests Using `assert_single_task_log_contains`

### test_override_job_user.py (9 calls)
| Test | Pattern | What it's really checking |
|------|---------|--------------------------|
| `test_no_user_override` | `I am: job-user` | Job ran as expected user |
| `test_config_file_user_override` | `I am: config-override` | Config override user works |
| `test_installer_user_override` | `I am: {WINDOWS_JOB_USER}` (×2) | Installer override user works |
| `test_env_var_user_override` | `I am: env-override` | Env var override user works |
| Linux: `test_no_user_override` | `I am: {posix_job_user}` | Job ran as expected user |
| Linux: `test_job_is_run_as_custom_worker_agent_user` | `I am: {CUSTOM_AGENT_NAME}` | Agent user override works |
| Linux: `test_config_file_user_override` | `I am: {config_override_user}` | Config override user works |
| Linux: `test_env_var_user_override` | `I am: {env_override_user}` | Env var override user works |

**Migration path:** Run `whoami` in the job and assert on job success + compare output via job attachments or task parameters (same pattern as `test_domain_user.py`).

### test_cap_kill.py (4 calls)
| Test | Pattern | What it's really checking |
|------|---------|--------------------------|
| `test_cap_kill_not_inherited_by_running_jobs` | Error message pattern | Job process doesn't have cap_kill |
| `test_worker_subprocesses_have_no_capabilities` | `Current: =\s*\n` | Capabilities are empty |
| `test_worker_subprocesses_have_no_capabilities` | `Ambient set =\s*\n` | Ambient caps are empty |
| `test_worker_only_has_cap_kill` | `\d+: cap_kill=ep\n` | Worker has only cap_kill |

**Migration path:** Write capability info to a file (job attachment output) and assert on file contents.

### test_job_submissions.py (4 calls)
| Test | Pattern | What it's really checking |
|------|---------|--------------------------|
| `test_worker_reports_canceled_session_actions_as_canceled` | `Hello!` | Environment script ran |
| `test_worker_run_with_number_of_environments` | `Hello!` (×3), repeated | Multiple environments ran |
| `test_worker_streams_logs_to_cloudwatch` | `HelloWorld` + worker log check | **Legitimately testing CW** |

**Migration path:** First two can use job result. Last one (`test_worker_streams_logs_to_cloudwatch`) should keep CW assertions — that's its purpose.

### test_installer.py (1 call)
| Test | Pattern | What it's really checking |
|------|---------|--------------------------|
| `test_custom_agent_runs_job_as_user` | `Jobs Run As: {DEFAULT_JOB_USER}` | Installer set correct job user |

**Migration path:** Same whoami pattern as override tests.

### test_credential_handling.py (1 call)
| Test | Pattern | What it's really checking |
|------|---------|--------------------------|
| `test_access_worker_credential_file_from_job_*` | `Error accessing credential files:` | Job can't access worker creds |

**Migration path:** Job already asserts `TaskStatus.FAILED`. The log pattern check confirms the *reason*. Could use task failure message from the API instead.

### test_job_attachments.py (2 calls)
| Test | Pattern | What it's really checking |
|------|---------|--------------------------|
| `test_job_submission_asset_sync_behaviour_expected_without_errors` | `Upload script executed successfully` | Upload ran |
| `test_job_submission_asset_sync_behaviour_expected_without_errors` | `Download script executed successfully` | Download ran |

**Migration path:** Assert on job success + verify output files exist in S3/locally.

### test_worker_config.py (1 call)
| Test | Pattern | What it's really checking |
|------|---------|--------------------------|
| `test_session_root_dir` | Session root dir path in output | Custom session root was used |

**Migration path:** Write `$OPENJD_SESSION_ROOT_DIR` to a file via job attachment output.

## Also: Direct CW polling (not via `assert_single_task_log_contains`)

### test_job_submissions.py — `test_worker_streams_logs_to_cloudwatch`
Polls `get_log_events` for worker logs. **Keep as-is** — this is legitimately testing CW log streaming.

## Summary

## Migration Progress

| File | CW assertions | Migrated | Must keep CW |
|------|:---:|:---:|:---:|
| test_override_job_user.py | 9 | ✅ 9 | 0 |
| test_cap_kill.py | 4 | ✅ 4 | 0 |
| test_job_submissions.py | 4 | ✅ 2 | 2 (`test_worker_streams_logs_to_cloudwatch`) |
| test_installer.py | 1 | ✅ 1 | 0 |
| test_credential_handling.py | 1 | ✅ 1 | 0 |
| test_job_attachments.py | 2 | ✅ 2 | 0 |
| test_worker_config.py | 1 | ✅ 1 | 0 |
