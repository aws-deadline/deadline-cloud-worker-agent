#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -eux

cd $HOME

#mkdir -p .aws/models/deadline
#cp -r /aws/models/deadline/* .aws/models/deadline/

python -m venv .venv
source .venv/bin/activate
pip install /code/dist/deadline_cloud_worker_agent-*-py3-none-any.whl

deadline-worker-agent \
    --no-shutdown \
    --farm-id $FARM_ID \
    --fleet-id $FLEET_ID
