#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -eux

# Set up the directories that the worker agent needs to write to.
mkdir -p /var/log/amazon/deadline
chown ${AGENT_USER}:${AGENT_USER} /var/log/amazon/deadline
chmod 700 /var/log/amazon/deadline
mkdir -p /var/lib/deadline
mkdir -p /sessions
chown ${AGENT_USER}:${SHARED_GROUP} /sessions
chmod 755 /sessions
# Shared directory for sharing credentials process with the job user.
mkdir -p /var/lib/deadline/queues
chown ${AGENT_USER}:${SHARED_GROUP} /var/lib/deadline /var/lib/deadline/queues
chmod 750 /var/lib/deadline /var/lib/deadline/queues

cp /config/run_agent.sh /home/${AGENT_USER}
chown ${AGENT_USER}:${AGENT_USER} /home/${AGENT_USER}/run_agent.sh
