# Logging

The AWS Deadline Cloud worker agent logs information about its operation to files on its host and to
[AWS CloudWatch Logs](https://docs.aws.amazon.com/cloudwatch/#amazon-cloudwatch-logs) as it is
running. There are two kinds of logs that it creates:

## Worker logs

These logs are intended to describe the activities of the worker. These are useful to understand
what the worker does, what information it has, what software package versions it runs, and other
information useful for diagnosis and troubleshooting workers.

The default locations for worker logs are:

| Platform | Default worker log path |
| --- | --- |
| Linux | `/var/log/amazon/deadline` |
| MacOS | `/var/log/amazon/deadline` |
| Windows | `C:\ProgramData\Amazon\Deadline\Logs` |

The worker agent maintains two sets of rotating log files:

*   `worker-agent-bootstrap.log` &mdash; Logs of solely the bootup phase of the Worker Agent.
*   `worker-agent.log` &mdash; Complete logs of the Worker Agent's operation.

The service creates and provides the worker with a conventionally-named AWS CloudWatch log group
and stream created in your account:

**Log group:** `/aws/deadline/<farm-id>/<fleet-id>`  
**Log stream:** `<worker-id>`

As the worker agent is running, it uploads its logs to this CloudWatch log stream.

### Structured worker logs

The worker logs emited to AWS CloudWatch Logs are structured, and the logs emited to local file and
stderr are unstructured by default. Each structured log event contains a single JSON-encoded
dictionary with information about the specific log event.

All log events structures contain:

- A `level` field that indicates the severity of the log event, following the typical Python Logger
  semantics: INFO, WARNING, ERROR, EXCEPTION, and CRITICAL.

Log events may also contain a `type`, `subtype`, icon (`ti`), and additional fields as indicated in the following table.

| type | subtype | ti | fields | purpose |
| --- | --- | --- | --- | --- |
| None | None | None | message | A simple status message or update and its log level. These messages may change at any time and must not be relied upon for automation. |
| Action | Start | 🟢 | session_id; queue_id; job_id; action_id; kind; message; step_id (optional); task_id (optional) | A SessionAction has started running. |
| Action | Cancel/Interrupt | 🟨 | session_id; queue_id; job_id; action_id; kind; message; step_id (optional); task_id (optional) | A cancel/interrupt of a SessionAction has been initiated. |
| Action | End | 🟣 | session_id; queue_id; job_id; action_id; kind; status; message; step_id (optional); task_id (optional) | A SessionAction has completed running. |
| AgentInfo | None | None | platform; python[interpreter,version]; agent[version,installedAt,runningAs]; depenencies | Information about the running Agent software. |
| API | Req | 📤 | operation; request_url; params; resource (optional) | A request to an AWS API. Only requests to AWS Deadline Cloud APIs contain a resource field. |
| API | Resp | 📥 | operation; params; status_code, request_id; error (optional) | A response from an AWS API request. |
| FileSystem | Read/Write/Create/Delete | 💾 | filepath; message | A filesystem operation. |
| AWSCreds | Load/Install/Delete | 🔑 | resource; message; role_arn (optional) | Related to an operation for AWS Credentials. |
| AWSCreds | Query | 🔑 | resource; message; role_arn (optional); expiry (optional) | Related to an operation for AWS Credentials. |
| AWSCreds | Refresh | 🔑 | resource; message; role_arn (optional); expiry (optional); scheduled_time (optional) | Related to an operation for AWS Credentials. |
| Metrics | System | 📊 | many | System metrics. |
| Session | Starting/Failed/AWSCreds/Complete/Info | 🔷 | queue_id; job_id; session_id | An update or information related to a Session. |
| Session | Add/Remove | 🔷 | queue_id; job_id; session_id; action_ids; queued_actions | Adding or removing SessionActions in a Session. |
| Session | Logs | 🔷 | queue_id; job_id; session_id; log_dest | Information regarding where the Session logs are located. |
| Session | User | 🔷 | queue_id; job_id; session_id; user | The user that a Session is running Actions as. |
| Session | Runtime | 🔷 | queue_id (optional); job_id (optional); session_id | Information related to the running Session. This includes information about the host, process control, and encountered Exceptions which could contain information like filepaths. |
| Worker | Create/Load/ID/Status/Delete | 💻 | farm_id; fleet_id; worker_id (optional); message | A notification related to a Worker resource within AWS Deadline Cloud. |

If you prefer structured logs to be emited on your host, then you can configure your Worker Agent to emit structured logs instead. Please see the
`structured_logs` option in the [`worker.toml.example`](../src/deadline_worker_agent/installer/worker.toml.example)
for information on how to configure your agent in this way.

## Worker session logs

These are the logs for the Job workloads that are run by the worker agent on the host. A separate
log file is created for each session that is run on the host, and is identified by that Session's
ID.

The default paths for worker logs are:

| Platform | Default session log Path |
| --- | --- |
| Linux | `/var/log/amazon/deadline/<queue-id>/<session-id>.log` |
| MacOS | `/var/log/amazon/deadline/<queue-id>/<session-id>.log` |
| Windows | `%PROGRAMDATA%\Amazon\Deadline\Logs\<queue-id>\<session-id>.log` |

The service creates and provides the worker with an AWS CloudWatch log group and stream for each
worker session created in your account. The CloudWatch log group and stream are:

**Log group name:** `/aws/deadline/<farm-id>/<queue-id>/`  
**Log stream name:** `<session-id>`

As the worker session runs, the agent uploads its logs to this CloudWatch log stream.
