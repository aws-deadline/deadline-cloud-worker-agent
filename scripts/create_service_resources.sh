#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -euo pipefail

# Default to us-west-2 unless another default region is set.
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-"us-west-2"}

if test $# -lt 1
then
    echo "Usage: $0 <Fleet Role Arn> <s3 bucket name> [<Queue#1 Role Arn> [<Queue#2 Role Arn]]"
    echo "where:"
    echo "  <Fleet Role Arn>: The ARN of the Worker Role to attach to the Fleet;"
    echo "                    this is used by the worker agent during operations."
    echo "  <s3 bucketname>: The name of an S3 bucket to configure on the Queues for use"
    echo "                   with the Job Attachments feature."
    echo "  <Queue* Role Arn>: The ARN of the Role whose credentials will be provided"
    echo "                      to the running jobs."
    exit 1
fi

# Parameters to take in
worker_iam_role=$1
assets_s3_bucket=$2
queue_1_iam_role=${3:-}
queue_2_iam_role=${4:-}

[ -f $(pwd)/.deployed_resources.sh ] && source $(pwd)/.deployed_resources.sh

if [ "${FARM_ID:-}" == "" ]
then
    echo "Enter a name for your Farm: "
    read farm_name
        
    echo "Creating AWS Deadline Cloud Farm $farm_name"
    FARM_ID=$(aws deadline create-farm --display-name $farm_name | jq -r ".farmId")
    echo "Created Farm: ${FARM_ID}"
fi

if [ "${QUEUE_ID_1:-}" == "" ]
then
    echo "Enter a name for your Queue #1: "
    read queue_name

    if test "${queue_1_iam_role}" == ""
    then
        cat << EOF > create-queue-config.json
{
    "farmId": "$FARM_ID",
    "displayName": "$queue_name",
    "jobRunAsUser": {
        "posix": {
            "user": "jobuser",
            "group": "jobuser"
        },
        "runAs": "QUEUE_CONFIGURED_USER"
    },
    "jobAttachmentSettings": {
        "s3BucketName": "${assets_s3_bucket}",
        "rootPrefix": "assets/"
    }
}
EOF
    else
        cat << EOF > create-queue-config.json
{
    "farmId": "$FARM_ID",
    "displayName": "$queue_name",
    "roleArn": "$queue_1_iam_role",
    "jobRunAsUser": {
        "posix": {
            "user": "jobuser",
            "group": "jobuser"
        },
        "runAs": "QUEUE_CONFIGURED_USER"
    },
    "jobAttachmentSettings": {
        "s3BucketName": "${assets_s3_bucket}",
        "rootPrefix": "assets/"
    }
}
EOF
    fi
        
    echo "Creating AWS Deadline Cloud Queue $queue_name"
    QUEUE_ID_1=$(aws deadline create-queue --cli-input-json file://create-queue-config.json | jq -r ".queueId")
    rm create-queue-config.json

    ready=""
    while [[ $ready != "IDLE" ]] && [[ $ready != "SCHEDULING" ]]; do
        sleep 5
        ready=$(aws deadline get-queue --farm-id $FARM_ID --queue-id $QUEUE_ID_1 | jq -r ".status")
        echo "Queue $QUEUE_ID_1 in $ready status..."
    done

    if [[ $ready == "DELETED" ]]; then
        echo "Failed to create a Queue!"
        exit 1
    fi
fi


if [ "${QUEUE_ID_2:-}" == "" ]
then
    echo "Enter a name for your Queue #2: "
    read queue_name

    if test "${queue_2_iam_role}" == ""
    then
        cat << EOF > create-queue-config.json
{
    "farmId": "$FARM_ID",
    "displayName": "$queue_name",
    "jobRunAsUser": {
        "posix": {
            "user": "jobuser",
            "group": "jobuser"
        },
        "runAs": "QUEUE_CONFIGURED_USER"
    },
    "jobAttachmentSettings": {
        "s3BucketName": "${assets_s3_bucket}",
        "rootPrefix": "assets/"
    }
}
EOF
    else
        cat << EOF > create-queue-config.json
{
    "farmId": "$FARM_ID",
    "displayName": "$queue_name",
    "roleArn": "$queue_2_iam_role",
    "jobRunAsUser": {
        "posix": {
            "user": "jobuser",
            "group": "jobuser"
        },
        "runAs": "QUEUE_CONFIGURED_USER"
    },
    "jobAttachmentSettings": {
        "s3BucketName": "${assets_s3_bucket}",
        "rootPrefix": "assets/"
    }
}
EOF
    fi
        
    echo "Creating AWS Deadline Cloud Queue $queue_name"
    QUEUE_ID_2=$(aws deadline create-queue --cli-input-json file://create-queue-config.json | jq -r ".queueId")
    rm create-queue-config.json

    ready=""
    while [[ $ready != "IDLE" ]] && [[ $ready != "SCHEDULING" ]]; do
        sleep 5
        ready=$(aws deadline get-queue --farm-id $FARM_ID --queue-id $QUEUE_ID_2 | jq -r ".status")
        echo "Queue $QUEUE_ID_2 in $ready status..."
    done

    if [[ $ready == "DELETED" ]]; then
        echo "Failed to create a Queue!"
        exit 1
    fi
fi

if [ "${FLEET_ID:-}" == "" ]
then
    echo "Enter a name for your Fleet: "
    read fleet_name

    # Customer managed fleet
    cat << EOF > create-fleet-config.json
{
    "farmId": "$FARM_ID",
    "displayName": "$fleet_name",
    "roleArn": "$worker_iam_role",
    "maxWorkerCount": 5,
    "configuration": {
        "customerManaged": {
            "mode": "NO_SCALING", 
            "workerRequirements": {
                "vCpuCount": {
                    "min": 1
                },
                "memoryMiB": {
                    "min": 512
                },
                "osFamily": "linux",
                "cpuArchitectureType": "x86_64"
            }
        }
    }
}
EOF

    echo "Creating AWS Deadline Cloud Fleet $fleet_name"
    FLEET_ID=$(aws deadline create-fleet --cli-input-json file://create-fleet-config.json | jq -r ".fleetId")
    rm create-fleet-config.json
        
    ready=""
    while [[ $ready != "READY" ]] && [[ $ready != "CREATE_FAILED" ]] && [[ $ready != "ACTIVE" ]]; do
        sleep 5
        ready=$(aws deadline get-fleet --farm-id $FARM_ID --fleet-id $FLEET_ID | jq -r ".status")
        echo "Fleet $FLEET_ID in $ready status..."
    done

    if [[ $ready == "CREATE_FAILED" ]]; then
        echo "Failed to create a Fleet!"
        exit 1
    fi
fi

aws deadline create-queue-fleet-association --farm-id $FARM_ID --queue-id $QUEUE_ID_1 --fleet-id $FLEET_ID
aws deadline create-queue-fleet-association --farm-id $FARM_ID --queue-id $QUEUE_ID_2 --fleet-id $FLEET_ID
    
echo "export FARM_ID=$FARM_ID" > .deployed_resources.sh
echo "export QUEUE_ID_1=$QUEUE_ID_1" >> .deployed_resources.sh
echo "export QUEUE_ID_2=$QUEUE_ID_2" >> .deployed_resources.sh
echo "export QUEUE_ID=\$QUEUE_ID_1" >> .deployed_resources.sh
echo "export FLEET_ID=$FLEET_ID" >> .deployed_resources.sh
cat .deployed_resources.sh
echo
echo "AWS Deadline Cloud setup complete!"
