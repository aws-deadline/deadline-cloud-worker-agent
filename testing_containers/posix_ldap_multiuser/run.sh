#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -eu

ENVS=""

for var in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_DEFAULT_REGION FARM_ID FLEET_ID
do
    
    ENVS="${ENVS} ${var}=${!var}"
done

if test "${PIP_INDEX_URL}" != ""; then
    ENVS="${ENVS} PIP_INDEX_URL=${PIP_INDEX_URL}"
fi

sudo -u agentuser -i ${ENVS} /home/agentuser/run_agent.sh