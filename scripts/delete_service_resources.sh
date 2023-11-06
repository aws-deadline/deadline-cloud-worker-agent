#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# The complement to create_service_resources.sh
# This deletes the service resources created by that script

set -euo pipefail

[ -f $(pwd)/.deployed_resources.sh ] && source $(pwd)/.deployed_resources.sh

delete_qfa() {
    farm_id=$1
    fleet_id=$2
    queue_id=$3

    aws deadline update-queue-fleet-association --farm-id $farm_id --fleet-id $fleet_id --queue-id $queue_id --status STOP_SCHEDULING_AND_CANCEL_TASKS
    status=""
    while [[ $status != "STOPPED" ]]; do
        sleep 5
        status=$(aws deadline get-queue-fleet-association --farm-id $farm_id --fleet-id $fleet_id --queue-id $queue_id  | jq -r ".status")
        echo "Queue $QUEUE_ID_1 in $status status... waiting for STOPPED status"
    done
    aws deadline delete-queue-fleet-association --farm-id $farm_id --fleet-id $fleet_id --queue-id $queue_id 
}

if [ "${FLEET_ID:-}" != "" ] && [ "${QUEUE_ID_1:-}" != "" ];
then
    delete_qfa $FARM_ID $FLEET_ID $QUEUE_ID_1
fi

if [ "${FLEET_ID:-}" != "" ] && [ "${QUEUE_ID_2:-}" != "" ];
then
    delete_qfa $FARM_ID $FLEET_ID $QUEUE_ID_2
fi

if [ "${FLEET_ID:-}" != "" ];
then
    aws deadline delete-fleet --farm-id $FARM_ID --fleet-id $FLEET_ID
fi
if [ "${QUEUE_ID_1:-}" != "" ];
then
    aws deadline delete-queue --farm-id $FARM_ID --queue-id $QUEUE_ID_1
fi
if [ "${QUEUE_ID_2:-}" != "" ];
then
    aws deadline delete-queue --farm-id $FARM_ID --queue-id $QUEUE_ID_2
fi

sleep 30

aws deadline delete-farm --farm-id $FARM_ID