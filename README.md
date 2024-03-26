# DeadlineWorkerAgent

This package has two active branches:

- `mainline` -- For active development. This branch is not intended to be consumed by other packages.
   Any commit to this branch may break APIs, dependencies, and so on, and thus break any consumer
   without notice.
- `release` -- The official release of the package intended for consumers. Any breaking releases will
   be accompanied with an increase to this package's interface version.

## Overview

The DeadlineWorkerAgent package contains the worker agent software that is installed on worker nodes
to interact with AWS Deadline Cloud and perform tasks.

## Versioning

This package's version follows [Semantic Versioning 2.0](https://semver.org/), but is still considered to be in its
initial development, thus backwards incompatible versions are denoted by minor version bumps. To help illustrate how
versions will increment during this initial development stage, they are described below:

1. The MAJOR version is currently 0, indicating initial development
2. The MINOR version is currently incremented when backwards incompatible changes are introduced to the public API.
3. The PATCH version is currently incremented when bug fixes or backwards compatible changes are introduced to the public API.

## Logging

The AWS Deadline Cloud Worker Agent logs information about its operation to files on its host and to
[AWS CloudWatch Logs](https://docs.aws.amazon.com/cloudwatch/#amazon-cloudwatch-logs) as it is running. There are two kinds of
logs that it creates:

1. Agent logs: These are logs to assist in understanding, troubleshooting, and debugging the operation of the Agent software itself.
    - Platform-specific locations:
        - Linux:
            - `/var/log/amazon/deadline/worker-agent-bootstrap.log`: Logs of solely the bootup phase of the Worker Agent.
            - `/var/log/amazon/deadline/worker-agent.log`: Complete logs of the Worker Agent's operation.
        - Windows:
            - `%PROGRAMDATA%\Amazon\Deadline\Logs\worker-agent-bootstrap.log`: Logs of solely the bootup phase of the Worker Agent.
            - `%PROGRAMDATA%\Amazon\Deadline\Logs\worker-agent.log`: Complete logs of the Worker Agent's operation.
    - AWS CloudWatch Logs -- all logs are also stored in CloudWatch Logs in your account.
        - Log group name: `/aws/deadline/<farm-id>/<fleet-id>/`
        - Log stream name: `<worker-id>`
2. Session Logs: These are the logs for the Job workloads that are run by the Worker Agent on the host. A separate log file is
   created for each Session that is run on the host, and is identified by that Session's ID.
    - Platform-specific locations:
        - Linux: `/var/log/amazon/deadline/<queue-id>/<session-id>.log`
        - Windows: `%PROGRAMDATA%\Amazon\Deadline\Logs\<queue-id>\<session-id>.log`
    - AWS CloudWatch Logs -- all logs are also stored in CloudWatch Logs in your account.
        - Log group name: `/aws/deadline/<farm-id>/<queue-id>/`
        - Log stream name: `<session-id>`

### Structured Agent Logs

The Agent logs emited to AWS CloudWatch Logs are structured, and the logs emited to local file and stdout are
unstructured by default. Each structured log event contains a single JSON-encoded dictionary
with information about the specific log event.

All log events structures contain:

- A `level` field that indicates the severity of the log event, following the typical Python Logger
  semantics: INFO, WARNING, ERROR, EXCEPTION, and CRITICAL.

Log events may also contain a `type`, `subtype`, icon (`ti`), and additional fields as indicated in the following table.

| type | subtype | ti | fields | purpose |
| --- | --- | --- | --- | --- |
| None | None | None | message | A simple status message or update and its log level. These messages may change at any time and must not be relied upon for automation. |
| Action | Start | 🟢 | session_id; queue_id; job_id; action_id; message | A SessionAction has started running. |
| Action | Cancel/Interrupt | 🟨 | session_id; queue_id; job_id; action_id; message | A cancel/interrupt of a SessionAction has been initiated. |
| Action | End | 🟣 | session_id; queue_id; job_id; action_id; status; message | A SessionAction has completed running. |
| AgentInfo | None | None | platform; python[interpreter,version]; agent[version,installedAt,runningAs]; depenencies | Information about the running Agent software. |
| API | Req | 📤 | operation; request_url; params; resource (optional) | A request to an AWS API. Only requests to AWS Deadline Cloud APIs contain a resource field. |
| API | Resp | 📥 | operation; params; status_code, request_id; error (optional) | A response from an AWS API request. |
| FileSystem | Read/Write/Create/Delete | 💾 | filepath; message | A filesystem operation. |
| AWSCreds | Load/Install/Delete | 🔑 | resource; message; role_arn (optional) | Related to an operation for AWS Credentials. |
| AWSCreds | Query | 🔑 | resource; message; role_arn (optional); expiry (optional) | Related to an operation for AWS Credentials. |
| AWSCreds | Refresh | 🔑 | resource; message; role_arn (optional); expiry (optional); scheduled_time (optional) | Related to an operation for AWS Credentials. |
| Metrics | System | 📊 | many | System metrics. |
| Session | Starting/Failed/AWSCreds/Complete/Info | 🔷 | queue_id; job_id; session_id | An update or information related to a Session. |
| Session | Add/Remove | 🔷 | queue_id; job_id; session_id; action_ids | Adding or removing SessionActions in a Session. |
| Session | Logs | 🔷 | queue_id; job_id; session_id; log_dest | Information regarding where the Session logs are located. |
| Session | User | 🔷 | queue_id; job_id; session_id; user | The user that a Session is running Actions as. |
| Worker | Create/Load/ID/Status/Delete | 💻 | farm_id; fleet_id; worker_id (optional); message | A notification related to a Worker resource within AWS Deadline Cloud. |

If you prefer structured logs to be emited on your host, then you can configure your Worker Agent to emit structured logs instead. Please see the
`structured_logs` option in the [example configuration file](https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/release/src/deadline_worker_agent/installer/worker.toml.example)
for information on how to configure your agent in this way.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## Telemetry

This library collects telemetry data by default. Telemetry events contain non-personally-identifiable information that helps us understand how users interact with our software so we know what features our customers use, and/or what existing pain points are.

You can opt out of telemetry data collection by either:

1. Setting the environment variable: `DEADLINE_CLOUD_TELEMETRY_OPT_OUT=true`
2. Providing the installer flag: `--telemetry-opt-out`
3. Setting the config file: `deadline config set telemetry.opt_out true`

Note that setting the environment variable supersedes the config file setting.

## License

This project is licensed under the Apache-2.0 License.
