#!/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

if ! which jq 2>&1 > /dev/null
then
    echo "ERROR: jq must be installed."
    exit 1
fi

if test $# -lt 1
then
    echo "Usage: $0 <IAM Role Arn>"
    exit 1
fi

unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
unset AWS_SESSION_TOKEN

export ASSUME_ROLE=$(aws sts assume-role --role-arn $1 --role-session-name WorkerAgentAssumeRole)
export AWS_ACCESS_KEY_ID=$(printenv ASSUME_ROLE | jq -r '.Credentials''.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(printenv ASSUME_ROLE | jq -r '.Credentials''.SecretAccessKey')
export AWS_SESSION_TOKEN=$(printenv ASSUME_ROLE | jq -r '.Credentials''.SessionToken')
unset ASSUME_ROLE