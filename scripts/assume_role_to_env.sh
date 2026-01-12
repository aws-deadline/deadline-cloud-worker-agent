#!/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

if ! which jq 2>&1 > /dev/null
then
    echo "ERROR: jq must be installed."
    return 1
fi

if test $# -lt 1
then
    echo "Usage: $0 <IAM Role Arn>"
    return 1
fi

unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
unset AWS_SESSION_TOKEN

ASSUME_ROLE=$(aws sts assume-role --role-arn "$1" --role-session-name WorkerAgentAssumeRole)
export ASSUME_ROLE

AWS_ACCESS_KEY_ID=$(printenv ASSUME_ROLE | jq -r '.Credentials''.AccessKeyId')
AWS_SECRET_ACCESS_KEY=$(printenv ASSUME_ROLE | jq -r '.Credentials''.SecretAccessKey')
AWS_SESSION_TOKEN=$(printenv ASSUME_ROLE | jq -r '.Credentials''.SessionToken')

export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN

unset ASSUME_ROLE

if [[ -z "$AWS_ACCESS_KEY_ID" ]]; then
    echo "ERROR: failed to set AWS_ACCESS_KEY_ID"
    return 1
fi
if [[ -z "$AWS_SECRET_ACCESS_KEY" ]]; then
    echo "ERROR: failed to set AWS_SECRET_ACCESS_KEY"
    return 1
fi
if [[ -z "$AWS_SESSION_TOKEN" ]]; then
    echo "ERROR: failed to set AWS_SESSION_TOKEN"
    return 1
fi

