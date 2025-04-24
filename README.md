# AWS Deadline Cloud Worker Agent

[![pypi](https://img.shields.io/pypi/v/deadline-cloud-worker-agent.svg)](https://pypi.python.org/pypi/deadline-cloud-worker-agent)
[![python](https://img.shields.io/pypi/pyversions/deadline-cloud-worker-agent.svg?style=flat)](https://pypi.python.org/pypi/deadline-cloud-worker-agent)
[![license](https://img.shields.io/pypi/l/deadline-cloud-worker-agent.svg?style=flat)](https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/mainline/LICENSE)

The AWS Deadline Cloud worker agent can be used to run a worker in an
[AWS Deadline Cloud][deadline-cloud] fleet. This includes managing the life-cycle of a worker and
its assigned work both in the service and on the worker's host.

Deadline Cloud schedules work as worker sessions which are an extension of
[Open Description (OpenJD)][openjd] sessions specific to AWS Deadline Cloud. The worker agent
initiates session actions, monitors them, and reports the status of running and completed session
actions to the service including progress, logs, process exit code, and indicates if the work was
canceled or interrupted.

The worker agent behavior follows the AWS Deadline Cloud [worker API protocol][protocol] that
specifies the expectation of how the service and workers behave and collaborate through Deadline
Cloud's worker APIs.

For guidance on setting up the worker agent for use in a customer-managed fleet, see the
["Manage Deadline Cloud customer-managed fleets"][manage-cmf-docs] topic in the AWS Deadline Cloud
User Guide 

[deadline-cloud]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/what-is-deadline-cloud.html
[manage-cmf-docs]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/manage-cmf.html
[openjd]: https://github.com/OpenJobDescription/openjd-specifications/wiki
[protocol]: https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/release/docs/dev/worker_api_protocol.md

## Compatibility

The worker agent requires Python 3.9 or higher. There are additional platform-specific requirements
listed below:

**Linux:**

*   Amazon Linux 2 and 2023 are recommended and tested
*   `sudo` must be installed

**Windows:**

*   Windows Server 2022 is recommended and tested
*   Requires CPython implementation of Python
*   **Python must be installed for all users** (e.g. in `C:\Program Files`)

**MacOS is intended to be used for testing only and is subject to change.**

## Versioning

This package's version follows [Semantic Versioning 2.0](https://semver.org/), but is still
considered to be in its initial development, thus backwards incompatible versions are denoted by
minor version bumps. To help illustrate how versions will increment during this initial development
stage, they are described below:

1. The MAJOR version is currently 0, indicating initial development
2. The MINOR version is currently incremented when backwards incompatible changes are introduced to the public API.
3. The PATCH version is currently incremented when bug fixes or backwards compatible changes are introduced to the public API.

## Installing program files

### Linux

We recommend installing the agent in a Python virtual environment (e.g. using [`venv`][venv]). For
this, run:

```sh
# Create venv
python -m venv /opt/deadline/worker

# Activate the virtual environment - you can later type "deactivate" to exit the environment
source /opt/deadline/worker/bin/activate

# Install worker agent program files into the virtual environment
pip install deadline-cloud-worker-agent
```

[venv]: https://docs.python.org/3/library/venv.html

### Windows

The worker agent runs as a Windows Service which leads to a few installation constraints:

*   Python virtual environments are not supported
*   Python must be installed for all users

To obtain the program files, run this command in an administrator command-prompt:

```sh
pip install deadline-cloud-worker-agent
```


## Setup worker host

The worker host can be prepared to be run using the provided `install-deadline-worker` command. This
command performs certain functions to setup the worker host based on arguments provided. The
command performs all worker host setup activities, such as:

*   creates an operating system user account (specified by the `--user` argument) on the worker
    host that the worker will run as. `install-deadline-worker` accepts a previously created user.
    The user defaults to `deadline-worker-agent` on Linux and `deadline-worker` on Windows.
*   creates a job user group (specified by `--group`, defaults to `deadline-job-users`) if required. The
    `install-deadline-worker` accepts an existing group.
*   creates cache, log, and config directories, and an example config file
*   [optionally] initializes the config file
*   modifies the config file using provided arguments
*   [optionally] install/update an operating system service
    *   [optionally] start the operating system service

---

**NOTE:** The `install-deadline-worker` command does not support MacOS at this time.

---

To see the available command-line arguments, run:

**On Linux:**

Assuming you have installed the worker agent to a Python venv in `/opt/deadline/worker`, run:

```sh
/opt/deadline/worker/bin/install-deadline-worker --help
```

**On Windows:**

Run the following command in an administrator command-prompt:

```bat
install-deadline-worker --help
``` 

## Configuration

[See configuration](https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/release/docs/configuration.md)

## State

[See state](https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/release/docs/state.md)

## Running

Setting up the worker host using the `install-deadline-worker` command (see "Setup worker host"
above) installs an operating system service. On Linux, this is a systemd service and on Windows this
is a Windows service.

The following commands demonstrate how to manually control the operating system service.

**On Linux:**

```sh
# Start the worker agent
systemctl start deadline-worker

# Stop the worker agent
systemctl stop deadline-worker

# Configure the worker agent to start on boot
systemctl enable deadline-worker

# Configure the worker agent to NOT start on boot
systemctl disable deadline-worker
```

**On Windows:**

Using an admin command-prompt:

```bat
REM start the service
sc.exe start DeadlineWorker

REM stop the service
sc.exe stop DeadlineWorker
```

### Running Outside of an Operating System Service

---
**NOTE:**
It is highly recommended to run the worker agent through an OS service in production environments. The OS service automatically handles:
- Restarting the worker agent process if the agent process crashes.
- Starting the worker agent when the host machine starts up.
- Attempting graceful shutdown when the host machine is shutting down.
----
The worker agent can also be started outside of a service context if required. Run `deadline-worker-agent --help` to see a list of supported command line arguments.

 **NOTE:** You must have an [AWS region](https://docs.aws.amazon.com/sdkref/latest/guide/feature-region.html) specified in order to run the worker agent from the command line. This can be configured through:
  - The `AWS_DEFAULT_REGION` [environment variable](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html#envvars-list).
  - The [AWS Configuration File](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-where)
    - Configured [manually](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-format), or through the [command line](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-methods).


## Logging

[See logging](https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/release/docs/logging.md)

## Contributing

See [`CONTRIBUTING.md`](https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/mainline/CONTRIBUTING.md)
for information on reporting issues, requesting features, and developer information.

## Security

See [security issue notifications](https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/release/CONTRIBUTING.md#security-issue-notifications) for more information.

## Telemetry

[See telemetry](https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/release/docs/telemetry.md)

## License

This project is licensed under the Apache-2.0 License.
