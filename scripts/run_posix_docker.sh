#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -eu

# Run this from the root of the repository
if ! test -d scripts
then
    echo "Must run from the root of the repository"
    exit 1
fi


USE_LDAP="False"
DO_BUILD="False"
BUILD_ONLY="False"
OVERRIDE_JOB_USER="False"
while [[ "${1:-}" != "" ]]; do
    case $1 in
        -h|--help)
            echo "Usage: run_sudo_tests.sh [--build]"
            exit 1
            ;;
        --build)
            DO_BUILD="True"
            ;;
        --ldap)
            echo "Using the LDAP client container image for testing."
            USE_LDAP="True"
            ;;
        --override-job-user)
            echo "Using the LDAP client container image which overrides the jobRunAsUSer"
            OVERRIDE_JOB_USER="True"
            ;;
        --build-only)
            BUILD_ONLY="True"
            ;;
        *)
            echo "Unrecognized parameter: $1"
            exit 1
            ;;
    esac
    shift
done

if test "${OVERRIDE_JOB_USER}" == "True" && test "${USE_LDAP}" == "True"; then
    echo "ERROR: Cannot use --ldap and --override-job-user together"
    exit 1
fi

# if ! test -d ${HOME}/.aws/models/deadline
# then
#     echo "ERROR: AWS Deadline Cloud service model must be installed to ~/.aws/models/deadline"
#     exit 1
# fi

ARGS=""

if test "${USE_LDAP}" == "True"; then
    ARGS="${ARGS} -h ldap.environment.internal"
    CONTAINER_IMAGE_TAG="agent_posix_ldap_multiuser"
    CONTAINER_IMAGE_DIR="posix_ldap_multiuser"
else
    ARGS="${ARGS} -h localuser.environment.internal"
    if test "${OVERRIDE_JOB_USER}" == "True"; then
        CONTAINER_IMAGE_TAG="agent_posix_local_multiuser"
        CONTAINER_IMAGE_DIR="posix_local_multiuser"
    else
        CONTAINER_IMAGE_TAG="agent_posix_local_multiuser_jobrunasuser"
        CONTAINER_IMAGE_DIR="posix_local_multiuser_jobRunAsUser"
    fi
fi

if test "${DO_BUILD}" == "True"; then
    docker build testing_containers/${CONTAINER_IMAGE_DIR} -t ${CONTAINER_IMAGE_TAG}
fi

if test "${BUILD_ONLY}" == "True"; then
    exit 0
fi

# Need to build the agent before we can run it
hatch clean
hatch build
chmod o+r dist/*.whl

if test "${AWS_ACCESS_KEY_ID:-}" == ""
then
    echo "ERROR: AWS Credentials for the Agent must be available in the environment".
    exit 1
fi

if test "${FARM_ID:-}" == "" || test "${FLEET_ID:-}" == ""
then
    echo "ERROR: AWS Deadline Cloud Farm & Fleet Ids must be available in the environment as FARM_ID and FLEET_ID"
    exit 1
fi

# Note: If you add any environment variables, then also add them to the exports in
# testing_containers/posix_ldap_multiuser/run.sh
TMP_ENV_FILE=$(mktemp -p $(pwd))
for var in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_DEFAULT_REGION FARM_ID FLEET_ID
do
    if test "${!var:-}" == "";
    then
        echo "ERROR: Environment variable ${var} must be set"
        exit 1
    fi
    echo -n "${var}=" >> $TMP_ENV_FILE
    printenv ${var} >> $TMP_ENV_FILE
done

if test "${PIP_INDEX_URL:-}" != ""; then
    echo "PIP_INDEX_URL=${PIP_INDEX_URL}" >> $TMP_ENV_FILE
fi

docker run --rm \
    --name test_worker_agent \
    -v $(pwd):/code:ro \
    -v ${HOME}/.aws:/aws \
    --env-file ${TMP_ENV_FILE} \
    ${ARGS} \
    ${CONTAINER_IMAGE_TAG}:latest 

rm -f ${TMP_ENV_FILE}
