#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# The complement to create_service_resources.sh
# This deletes the service resources created by that script

set -xeuo pipefail

[ -f $(pwd)/.deployed_resources.sh ] && source $(pwd)/.deployed_resources.sh

if [ "${FLEET_ID:-}" != "" ] && [ "${QUEUE_ID_1:-}" != "" ];
then
    aws deadline update-queue-fleet-association --farm-id $FARM_ID --fleet-id $FLEET_ID --queue-id $QUEUE_ID_1 --status CANCEL_WORK
    sleep 30
    aws deadline delete-queue-fleet-association --farm-id $FARM_ID --fleet-id $FLEET_ID --queue-id $QUEUE_ID_1
fi

if [ "${FLEET_ID:-}" != "" ] && [ "${QUEUE_ID_2:-}" != "" ];
then
    aws deadline update-queue-fleet-association --farm-id $FARM_ID --fleet-id $FLEET_ID --queue-id $QUEUE_ID_2 --status CANCEL_WORK
    sleep 30
    aws deadline delete-queue-fleet-association --farm-id $FARM_ID --fleet-id $FLEET_ID --queue-id $QUEUE_ID_2
fi

sleep 30

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