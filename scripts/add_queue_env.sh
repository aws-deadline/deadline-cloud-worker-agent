#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

ENV_JSON=$(dirname "$0")/queue-env.json
export ENV_JSON
aws deadline create-queue-environment \
    --farm-id "$FARM_ID" --queue-id "$QUEUE_ID" --priority 50 \
    --template-type JSON --template "$(<"${ENV_JSON}")"