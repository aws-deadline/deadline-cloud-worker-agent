# Configuration

The Worker agent configuration is loaded when the program starts. The configuration system has
three configuration sources which are checked in the following order:

1.  Command-line arguments
2.  Environment variables
3.  Configuration file
4.  Default (if available)

## 1. Command-line arguments

The agent can be run with command-line arguments. To see the list of available arguments, run:

```
deadline-worker-agent --help
```

## 2. Environment variables

The agent accepts environment variables of the form `DEADLINE_WORKER_<SETTING_NAME>`.

See [`worker.toml.example`](../src/deadline_worker_agent/installer/worker.toml.example) for details
about the supported environment variables.

## 3.  Config file

Configuration is stored in the following directories by default:

| Platform | Default config directory |
| --- | --- |
| Linux | `/etc/amazon/deadline` |
| MacOS | `/etc/amazon/deadline` |
| Windows | `C:\ProgramData\Amazon\Deadline\Config` |

When running `install-deadline-worker`, an example configuration file `worker.toml.example` is
installed to the configuration directory. If a `worker.toml` config file does not pre-exist (e.g.
from a prior setup), it will create one by copying the installed example file. The
`install-deadline-worker` command then modifies the configuration file based on its program
arguments.

Consult [`worker.toml.example`](../src/deadline_worker_agent/installer/worker.toml.example) which
contains embedded documentation in comments.

## AWS Region

The AWS region can be configured using any of the following methods, listed in priority order:

1.  boto3 default region resolution (`AWS_DEFAULT_REGION`, `AWS_REGION` environment variables,
    AWS config file, EC2 instance metadata)
2.  `region` setting in the `[aws]` section of `worker.toml`

The config file region is only used as a fallback when boto3 cannot resolve a region on its own.

To set the region in the config file, add the following to `worker.toml`:

```toml
[aws]
region = "us-west-2"
```