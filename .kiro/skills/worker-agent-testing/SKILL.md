---
name: worker-agent-testing
description: Run unit, integration, and E2E tests, linting, and formatting. Use when running tests or debugging test issues.
tags: [deadline-cloud, worker, worker-agent, testing, unit-test, integration-test, e2e-test, hatch, pytest, docker, live-service]
---

# Deadline Cloud Worker Agent Testing

This skill provides guidance for running tests in the `deadline-cloud-worker-agent` repository.

## Unit Tests

Run unit tests with:
```sh
hatch run test
```

Run tests across all supported Python versions:
```sh
hatch run all:test
```

## Linting & Formatting

```sh
hatch run lint
hatch run fmt
```

## Integration Tests

Integration tests run locally on the host machine and cover OS/filesystem integration.
If changes apply to both Windows and Linux, test on both platforms.

```sh
hatch run integ-test
```

## E2E Tests

E2E tests run the agent on EC2 instances against the live Deadline Cloud service. Tests are under `test/e2e/`.

### Prerequisites

1. Configure AWS credentials and region:
   ```sh
   export AWS_PROFILE=<your-profile>
   export AWS_DEFAULT_REGION=<region>
   ```
2. Deploy the S3 DeleteBucketContents CloudFormation resource to your account:
   https://github.com/aws-cloudformation/community-registry-extensions/blob/main/resources/S3_DeleteBucketContents/resource-role-prod.yaml
   Note the output role ARN.
3. Activate `AwsCommunity::S3::DeleteBucketContents` in AWS Console → CloudFormation → Public Extensions (use the role ARN from step 2).
4. Ensure your account has sufficient Farm quota (limit: 2 per account).
5. Deploy testing infrastructure:
   ```sh
   scripts/deploy_e2e_testing_infrastructure.sh
   ```
6. Gather environment variable exports per OS:
   ```sh
   ./scripts/get_e2e_test_ids_from_cfn.sh --os Linux > .e2e_linux_infra.sh
   ./scripts/get_e2e_test_ids_from_cfn.sh --os Windows > .e2e_windows_infra.sh
   ```

### Running E2E Tests

```sh
rm -f dist/*
hatch build
export WORKER_AGENT_WHL_PATH=$(pwd)/$(ls dist/*.whl)

# Linux
source .e2e_linux_infra.sh
hatch run e2e:test

# Windows
source .e2e_windows_infra.sh
hatch run e2e:test
```

### Using Local Wheel Files for Dependencies

You can override the `openjd-sessions` and/or `deadline-cloud` packages installed on the worker by pointing to local wheel files. This is useful when testing against unreleased or locally-built versions of those packages.

Both variables accept a path (glob patterns are supported, but must resolve to exactly one file). The wheel is copied to the worker's temp directory and installed via pip alongside the worker agent.

```sh
# Override openjd-sessions with a local build
export OPENJD_SESSIONS_WHL_PATH=/path/to/openjd_sessions-*.whl

# Override deadline-cloud with a local build
export DEADLINE_WHL_PATH=/path/to/deadline_cloud-*.whl
```

These can be combined with `WORKER_AGENT_WHL_PATH`:

```sh
export WORKER_AGENT_WHL_PATH=$(pwd)/$(ls dist/*.whl)
export OPENJD_SESSIONS_WHL_PATH=/path/to/openjd_sessions-*.whl
export DEADLINE_WHL_PATH=/path/to/deadline_cloud-*.whl
hatch run e2e:test
```

### Debugging Pytest Hanging

If pytest hangs after tests complete (likely due to a non-exiting Python thread), enable thread stack debugging:
```sh
export DEBUG_THREAD_STACKS=1
hatch run e2e:test
```

## Live Service Testing (Manual)

### Required AWS Resources

- A Farm, Fleet, and Queue
- A Bootstrapping Role (needs `CreateWorker` and `GetWorkerIamCredentials` permissions; trust policy allows your account to assume it)
- A Worker/Fleet Role (trust policy allows assume-role by the service's credential-vending service principal)
- Optionally, a Queue Role

Use the helper script to create non-role resources:
```sh
scripts/create_service_resources.sh <worker role arn> <queue role arn>
```

### Running the Agent via Docker

1. Install the service model: `aws configure add-model --service-name deadline ...`
   > **Important:** Use `--service-name deadline` (not any other name). The container mounts `~/.aws` as a
   > volume and symlinks it so boto3 can find the model at its default search path. This is required when
   > testing against internal or beta endpoints whose URLs differ from the public service.
   > If testing against the public Deadline Cloud service, skip this step.
2. Set env vars:
   ```sh
   export FARM_ID=<farm-id>
   export FLEET_ID=<fleet-id>
   export AWS_DEFAULT_REGION=<region>
   # Bootstrapping role credentials:
   source scripts/assume_role_to_env.sh <role arn>
   ```
3. Run the agent:
   ```sh
   scripts/run_posix_docker.sh --build
   ```
4. Stop the agent:
   ```sh
   docker exec test_worker_agent /home/agentuser/term_agent.sh
   ```
