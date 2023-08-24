#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -eo

service slapd start
service nscd restart
service nslcd restart
