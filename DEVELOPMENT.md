# Development Documentation

This documentation provides guidance on developer workflows for working with the code in this repository.

## Code Organization

### `src/deadline_worker_agent`

This is the root of the source code. The primary code files where most of the changes would be made exist in this root directory.

Files of note include:

*   `worker.py`

    `Worker` class implementation containing the main thread's event loop that runs after the Worker has been bootstrapped. This is the original implementation of the Worker Agent that uses `UpdateWorkerSchedule` and `NotifyProgress` which are APIs that are unaware of Worker Sessions.

## `src/deadline_worker_agent/boto`

This contains logic for boto3 and botocore.

### `src/deadline_worker_agent/config`

This Python sub-package contains modules for the worker agent's configuration settings system.

### `src/deadline_worker_agent/startup`

This contains logic for the startup phase in the Worker Agent's lifecycle.

### `src/deadline_worker_agent/log_sync`

This Python sub-package contains code responsible for synchronizing logs emitted by AWS Deadline Cloud tasks to their destination(s) in S3 and CloudWatch Logs, and synchronizing logs emitted by agent to CloudWatch Logs.

### `src/deadline_worker_agent/scheduler`

This contains an impementation of the Worker Agent's scheduler. This works with the AWS Deadline Cloud farm's scheduler via `UpdateWorkerSchedule` to synchronize the assignment, completion, and status reporting of work.

### `src/deadline_worker_agent/sessions`

This contains the logic and APIs for managing the life-cycle of a Worker session. The primary class contained in this package, the `Session` class, is responsible for taking actions from the `SessionActionQueue` and running them within the Open Job Description session.

### `src/deadline_worker_agent/sessions/actions`

This package contains classes corresponding to each action and the logic for running them within the `Session`.

### `src/deadline_worker_agent/sessions/job_entities`

This package contains code responsible for fetching the job entities required for running Worker session actions. This coordinates efficient use of the `BatchGetJobEntity` API and provides a high-level API for asynchronously requesting (optionally in a batch) and waiting for fetched the entities.

### `src/deadline_worker_agent/installer`

This contains the logic for the `install_deadline_worker` entrypoint which provisions OS users, groups, sudoers rule, and file-system
directories used by the Worker Agent. Finally it configures a systemd service on Linux systems that runs the Worker Agent
on boot and restarts the process if it crashes unexpectedly.

## Build / Test

### Build the package.
```
hatch build
```

### Run unit tests
```
hatch run test
```

### Run linting
```
hatch run lint
```

### Run formating
```
hatch run fmt
```

### Run tests for all supported Python versions.
```
hatch run all:test
```

## Testing the agent with the live service

### Setup

To test the agent with the live service you will need to create a Farm, Fleet, and Queue. You will also
need two IAM Roles:

1. A Bootstrapping Role.
    * This needs allow permissions for CreateWorker and GetWorkerIamCredentials.
    * Its trust policy should allow your account to assume it.
2. A Worker Role (aka: Fleet Role).
    * Its trust policy needs to allow assume-role by the service's credential-vending service principal.
    * See service documentation for the permissions that this role requires.
3. Optionally, a Queue Role
    * Its trust policy needs to allow assume-role by the service's credential-vending service principal.
    * The permissions granted by this role can be anything that you want your submitted jobs to have available
      to them. We'd suggest minimimally having an empty-permissions role so that the assume-role code paths
      are tested.
 
There is a helper script at `script/create_service_resources.sh` to help you create the non-role service
resources. To run it, simply run:
```
# <worker role arn> is the ARN of the Worker Role that you created.
# <queue role arn> is the ARN of the Queue Role that you created
scripts/create_service_resources.sh <worker role arn> <queue role arn>
```

### Running the Worker Agent

We have created a docker container image, and a helper shell script for running it, that can be used for
testing of the Agent in an isolated environment.

To use it:

0. Ensure that your service model is installed as the `deadline` service name (`aws configure add-model --service-name deadline ...`)
1. Set `FARM_ID` and `FLEET_ID` environment variables to the ID of the Farm and Fleet that you created for testing.
    Note: If you used the `create_service_resources.sh` script, then you can simply `source .deployed_resources.sh`
2. Set the `AWS_DEFAULT_REGION` environment variable to contain the region code for the region containing your service resources.
3. Set the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` environment variables to contain credentials for
   your Bootstrapping Role.
    Note: The easiest way to do this is to set `AWS_DEFAULT_PROFILE` to a credentials profile that can assume your Bootstrapping Role,
    and then use the provided helper script to assume your bootstrapping role: `source scripts/assume_role_to_env.sh <role arn>`

Then, simply run the worker agent with:
```
scripts/run_posix_docker.sh --build
```

To stop the agent, simply run:
```
docker exec test_worker_agent /home/agentuser/term_agent.sh
```

### Running Worker Agent E2E Tests

The worker agent has end-to-end tests that run the agent on ec2 instances with the live Deadline Cloud service. These tests
are located under `test/e2e` in this repository. To run these tests:

1. Configure your AWS credentials profile & region to test within. (e.g. Set the env vars `AWS_PROFILE` and `AWS_DEFAULT_REGION`)
2. Deploy the testing infrastructure: Run `scripts/deploy_e2e_testing_infrastructure.sh`
3. Gather the environment variable exports that you will need for each OS:
```bash
./scripts/get_e2e_test_ids_from_cfn.sh --os Linux > .e2e_linux_infra.sh
./scripts/get_e2e_test_ids_from_cfn.sh --os Windows > .e2e_windows_infra.sh
```
4. Run the tests:
```
rm -f dist/*
hatch build
export WORKER_AGENT_WHL_PATH=$(pwd)/$(ls dist/*.whl)

# Linux
source .e2e_linux_infra.sh
hatch run e2e-test

# Windows
source .e2e_windows_infra.sh
hatch run e2e-test
```
