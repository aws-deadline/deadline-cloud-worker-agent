# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# This is template configuration file for configuring deadline-cloud-test-fixtures
# This is based on: https://github.com/casillas2/deadline-cloud-test-fixtures/blob/mainline/src/deadline_test_scaffolding/example_config.sh
# Usage: . ./config.sh

echo "You should not modify this file directly. Please create a copy outside of the git repository."
exit 1


# If "true", the Docker worker is used instead of the EC2 worker. Default is to use the EC2 worker.
# For EC2 Worker configuration, see the "EC2 WORKER OPTIONS" section below
# For Docker Worker configuration, see the "DOCKER WORKER OPTIONS" section below
export USE_DOCKER_WORKER


# ====================== #
# === COMMON OPTIONS === #
# ====================== #

# --- REQUIRED --- #

# The AWS account ID to deploy infrastructure into
export SERVICE_ACCOUNT_ID

# CodeArtifact repository information to configure pip to pull Python dependencies from
#
# The domain owner AWS account ID
export CODEARTIFACT_ACCOUNT_ID
# The domain the repository is in
export CODEARTIFACT_DOMAIN
# The name of the repository
export CODEARTIFACT_REPOSITORY
# The region the repository is in
export CODEARTIFACT_REGION

# --- OPTIONAL --- #

# Extra local path for boto to look for AWS models in
# Does not apply to the worker
export AWS_DATA_PATH

# Local path to the Worker agent .whl file to use for the tests
# Default is to pip install the latest "deadline-cloud-worker-agent" package
export WORKER_AGENT_WHL_PATH

# The AWS region to configure the worker for
# Falls back to AWS_DEFAULT_REGION, then defaults to us-west-2
export WORKER_REGION

# The POSIX user to configure the worker for
# Defaults to "deadline-worker"
export WORKER_POSIX_USER

# The shared POSIX group to configure the worker user and job user with
# Defaults to "shared-group"
export WORKER_POSIX_SHARED_GROUP

# PEP 508 requirement specifier for the Worker agent package
# If WORKER_AGENT_WHL_PATH is provided, this option is ignored
export WORKER_AGENT_REQUIREMENT_SPECIFIER

# The S3 URI for the "deadline" service model to use for the tests
# Falls back to LOCAL_MODEL_PATH, then defaults to your locally installed service model
export DEADLINE_SERVICE_MODEL_S3_URI

# Path to a local Deadline model file to use for API calls
# If DEADLINE_SERVICE_MODEL_S3_URI is provided, this option is ignored
# Default is to use the locally installed service model on your machine
export LOCAL_MODEL_PATH

# The endpoint to use for requests to the Amazon Deadline Cloud service
# Default is the endpoint specified in your AWS model file for "deadline"
export DEADLINE_ENDPOINT

# The CredentialVending service principal to configure the Worker IAM roles with
# If you don't know what this is, then you probably don't need to provide this
export CREDENTIAL_VENDING_PRINCIPAL

# If set to "true", does not stop the worker after test failure. Useful for debugging.
export KEEP_WORKER_AFTER_FAILURE



# If BYO_DEADLINE is "true", uses existing Deadline resource IDs as specified below
# By default, new resources are deployed for you that get deleted after test runs
export BYO_DEADLINE
# Required - The ID of the farm to use
export FARM_ID
# Required - The ID of the queue to use
export QUEUE_ID
# Required - The ID of the fleet to use
export FLEET_ID
# Optional - The ID of the KMS key association with your farm
# If you use this option, then you must BYO_BOOTSTRAP because the default IAM role created for
# the Worker will not have sufficient permissions to access this key
export FARM_KMS_KEY_ID
# Optional - The name of the S3 buckets to use for Job Attachments
export JOB_ATTACHMENTS_BUCKET



# If BYO_BOOTSTRAP is "true", uses existing bootstrap resources as specified below
# By default, new resources are deployed for you in a CloudFormation stack.
# This stack is not destroyed automatically after test runs.
export BYO_BOOTSTRAP
# Required - The name of the S3 bucket to use for bootstrapping files
export BOOTSTRAP_BUCKET_NAME
# Required - ARN of the IAM role to use for the Worker
export WORKER_ROLE_ARN
# Optional - ARN of the IAM role to use for sessions running on the Worker
export SESSION_ROLE_ARN
# Optional - Name of the IAM instance profile to bootstrap the Worker instance with
# This option does not apply if you USE_DOCKER_WORKER
export WORKER_INSTANCE_PROFILE_NAME



# ========================== #
# === EC2 WORKER OPTIONS === #
# ========================== #

# --- REQUIRED --- #

# Subnet to deploy the EC2 instance into
export SUBNET_ID

# Security group to deploy the EC2 instance into
export SECURITY_GROUP_ID

# --- OPTIONAL --- #

# AMI ID to use for the EC2 instance
# Defaults to latest AL2023 AMI
export AMI_ID



# ============================= #
# === DOCKER WORKER OPTIONS === #
# ============================= #

# --- REQUIRED --- #

# The AWS credentials the Worker agent will use to bootstrap itself
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN
export AWS_DEFAULT_REGION

# --- OPTIONAL --- #

# The pip index to use for installing Worker agent dependencies
# See https://pip.pypa.io/en/stable/cli/pip_install/
export PIP_INDEX_URL
