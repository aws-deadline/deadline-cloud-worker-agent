#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Prereqs:
#  1) Deploy https://github.com/aws-cloudformation/community-registry-extensions/blob/main/resources/S3_DeleteBucketContents/resource-role-prod.yaml to your
#     account.
#  2) AWS Console -> CloudFormation -> Public Extensions -> Search for Third Party Resource: 'AwsCommunity::S3::DeleteBucketContents' -> Activate

set -eou pipefail

if ! aws cloudformation describe-type --type RESOURCE --type-name "AwsCommunity::S3::DeleteBucketContents" > /dev/null;
then
    echo "You must register the AwsCommunity::S3::DeleteBucketContents before proceeding. See the header of this script for instructions."
    exit 1
fi

if ! aws cloudformation describe-stacks --stack-name DeadlineCloudAgentE2EInfrastructure 2>&1 > /dev/null; then
  OP=create-stack
else
  OP=update-stack
fi

aws cloudformation $OP --stack-name DeadlineCloudAgentE2EInfrastructure --template-body file://$(dirname $0)/e2e_testing_infrastructure.yaml --capabilities CAPABILITY_NAMED_IAM