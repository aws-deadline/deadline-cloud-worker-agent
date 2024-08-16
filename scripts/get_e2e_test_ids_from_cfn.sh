#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -eou pipefail

OS=""
while [[ "${1:-}" != "" ]]; do
    case $1 in
        -h|--help)
            echo "Usage: $(basename $0) --os (Linux | Windows)"
            exit 1
            ;;
        --os)
            shift
            case $1 in
                Linux)
                    OS="Linux"
                    ;;
                Windows)
                    OS="Windows"
                    ;;
                *)
                    echo "Unknown OS ($1). Valid options are: Linux, Windows"
                    exit 1
                    ;;
            esac
            ;;
        *)
            echo "Unrecognized parameter: $1"
            exit 1
            ;;
    esac
    shift
done

if test "$OS" = ""; then
    echo "Usage: $(basename $0) --os (Linux | Windows)"
    exit 1
fi

if ! aws cloudformation describe-stacks --stack-name DeadlineCloudAgentE2EInfrastructure 2>&1 > /dev/null; then
    echo "ERROR: You must deploy the testing infrastructure first."
    echo "See: scripts/deploy_e2e_testing_infrastructure.sh"
    exit 1
fi

# Get the outputs from the stack, and reformat into a dictionary
STACK_OUTPUTS=$(
    aws cloudformation describe-stacks --stack-name DeadlineCloudAgentE2EInfrastructure --query 'Stacks[0].Outputs' | \
    python -c 'import sys, json; raw=json.load(sys.stdin); d={ e["OutputKey"]: e["OutputValue"] for e in raw }; print(json.dumps(d))'
)

cat << EOF
export BYO_BOOTSTRAP=true
export OPERATING_SYSTEM=$(echo $OS | tr '[:upper:]' '[:lower:]')
export SUBNET_ID=$(echo ${STACK_OUTPUTS} | jq -r '.SubnetId')
export SECURITY_GROUP_ID=$(echo ${STACK_OUTPUTS} | jq -r '.SecurityGroupId')
export WORKER_INSTANCE_TYPE=t3.large

export CODEARTIFACT_ACCOUNT_ID=$(echo ${STACK_OUTPUTS} | jq -r '.Account')
export CODEARTIFACT_REGION=$(echo ${STACK_OUTPUTS} | jq -r '.Region')
export CODEARTIFACT_DOMAIN=$(echo ${STACK_OUTPUTS} | jq -r '.CodeArtifactDomainName')
export CODEARTIFACT_REPOSITORY=$(echo ${STACK_OUTPUTS} | jq -r '.CodeArtifactRepositoryName')

export CREDENTIAL_VENDING_PRINCIPAL=credentials.deadline.amazonaws.com

export BOOTSTRAP_BUCKET_NAME=$(echo ${STACK_OUTPUTS} | jq -r '.FixturesBucketName')
export JOB_ATTACHMENTS_BUCKET=$(echo ${STACK_OUTPUTS} | jq -r '.JobAttachmentsBucket')

export SESSION_ROLE=$(echo ${STACK_OUTPUTS} | jq -r '.QueueRoleArn')
export BOOTSTRAP_ROLE_ARN=$(echo ${STACK_OUTPUTS} | jq -r ".${OS}HostRoleArn")
export WORKER_INSTANCE_PROFILE_NAME=$(echo ${STACK_OUTPUTS} | jq -r ".${OS}WorkerInstanceProfileName")
export WORKER_ROLE_ARN=$(echo ${STACK_OUTPUTS} | jq -r ".${OS}WorkerRoleArn")

export FARM_ID=$(echo ${STACK_OUTPUTS} | jq -r '.FarmId')
export QUEUE_A_ID=$(echo ${STACK_OUTPUTS} | jq -r '.QueueAId')
export QUEUE_B_ID=$(echo ${STACK_OUTPUTS} | jq -r '.QueueBId')
export SESSION_FLEET_ID=$(echo ${STACK_OUTPUTS} | jq -r ".${OS}ManualSessionFleetX86Id")
export FUNCTION_FLEET_ID=$(echo ${STACK_OUTPUTS} | jq -r ".${OS}ManualFunctionFleetX86Id")
export SCALING_QUEUE_ID=$(echo ${STACK_OUTPUTS} | jq -r '.ScalingQueueId')
export SCALING_FLEET_ID=$(echo ${STACK_OUTPUTS} | jq -r ".${OS}AutoFleetX86Id")
EOF