#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Encapulating a one-liner for getting a bash shell in a running testing docker
# container.

docker exec -it test_worker_agent /bin/bash