# Development Documentation

This documentation provides guidance on developer workflows for working with the code in this repository.

## Code Organization

See [code organization](./docs/dev/architecture.md#3-code-organization).

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

### Running Worker Agent Integration Tests

The worker agent has integration tests that run locally on the host machine they are run from.
These tests cover integration with the host operating system and file-system. If you are making
changes that apply to both Windows and Linux, you will need to test your changes on both a Linux
host and a Windows host.

To run the tests, run:

```sh
hatch run integ-test
```

### Running Worker Agent E2E Tests

The worker agent has end-to-end tests that run the agent on ec2 instances with the live Deadline Cloud service. These tests
are located under `test/e2e` in this repository. To run these tests:

1. Configure your AWS credentials profile & region to test within. (e.g. Set the env vars `AWS_PROFILE` and `AWS_DEFAULT_REGION`)
2. Deploy https://github.com/aws-cloudformation/community-registry-extensions/blob/main/resources/S3_DeleteBucketContents/resource-role-prod.yaml to your account. Note down the output role ARN.
3. Goto `AWS Console -> CloudFormation -> Public Extensions -> Search for Third Party Resource: 'AwsCommunity::S3::DeleteBucketContents' -> Activate`. Use the role ARN from step 2.
4. Before deploying the test farm, make sure your account has sufficient Farm quota. Each account has a limit of 2.
5. Deploy the testing infrastructure: Run `scripts/deploy_e2e_testing_infrastructure.sh`
6. Gather the environment variable exports that you will need for each OS:
```bash
./scripts/get_e2e_test_ids_from_cfn.sh --os Linux > .e2e_linux_infra.sh
./scripts/get_e2e_test_ids_from_cfn.sh --os Windows > .e2e_windows_infra.sh
```
7. Run the tests:
```
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

You can also override the `openjd-sessions` and/or `deadline-cloud` packages installed on the worker
by pointing to local wheel files. This is useful when testing against unreleased or locally-built versions.
Both variables accept a path (glob patterns are supported, but must resolve to exactly one file).

```
export OPENJD_SESSIONS_WHL_PATH=/path/to/openjd_sessions-*.whl
export DEADLINE_WHL_PATH=/path/to/deadline_cloud-*.whl
```

#### Debugging Pytest Hanging

Sometimes you may encounter an issue where the tests complete, but pytest hangs and does not exit.
This could happen if the test code or the code-under-test creates a Python thread that does not exit.
There are two pytest hooks `pytest_unconfigure` and `pytest_sessionfinish` that have been
instrumented with debug tooling for this situation. To use this, set the `DEBUG_THREAD_STACKS`
environment variable before running the end-to-end tests.
