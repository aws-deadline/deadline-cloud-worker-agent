#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -eux

cd $HOME

python -m venv .venv
source .venv/bin/activate
pip install /code/dist/deadline_cloud_worker_agent-*-py3-none-any.whl

ln -s /aws ~/.aws

deadline-worker-agent \
    --posix-job-user jobuser:sharedgroup \
    --no-shutdown \
    --farm-id $FARM_ID \
    --fleet-id $FLEET_ID
