#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

export JOB_JSON=$(dirname "$0")/sleep_job.json
aws deadline create-job --farm-id "$FARM_ID" --queue-id "$QUEUE_ID" --priority 50 --template "$(<"${JOB_JSON}")" --template-type JSON
# Uncomment and add to the above command if you want to change the duration
# --parameters '{ "duration": { "int": "10" } }'

