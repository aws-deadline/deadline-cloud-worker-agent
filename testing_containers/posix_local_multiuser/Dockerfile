# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

FROM public.ecr.aws/docker/library/python:3.9-buster

ENV AGENT_USER=agentuser
ENV JOB_USER=jobuser
ENV SHARED_GROUP=sharedgroup

# Use a docker volume to mount the root of the repo to this directory
VOLUME /code /aws

# We set up two users:
#  1) agentuser -- the user that will be running the Worker Agent.
#  2) jobuser -- the user that we'll be running Jobs as.

# These accounts belong to the following groups:
#   agentuser: agentuser, sharedgroup
#   jobuser: jobuser, sharedgroup

RUN chmod 777 /code /aws && \
    apt-get update && apt-get install sudo && \
    rm -rf /var/lib/apt/lists/* && \
    addgroup ${SHARED_GROUP} &&  \
    useradd -ms /bin/bash -G ${SHARED_GROUP} ${JOB_USER} && \
    useradd -ms /bin/bash -G ${SHARED_GROUP} ${AGENT_USER} && \
    echo "${AGENT_USER} ALL=(${JOB_USER},${AGENT_USER}) NOPASSWD: ALL" > /etc/sudoers.d/${AGENT_USER} && \
    # Set up the directories that the worker agent needs to write to.
    mkdir -p /var/log/amazon/deadline && \
    chown ${AGENT_USER}:${AGENT_USER} /var/log/amazon/deadline && \
    chmod 700 /var/log/amazon/deadline && \
    mkdir -p /var/lib/deadline && \
    # Shared directory for sharing credentials process with the job user.
    mkdir -p /var/lib/deadline/queues && \
    chown ${AGENT_USER}:${SHARED_GROUP} /var/lib/deadline /var/lib/deadline/queues && \
    chmod 750 /var/lib/deadline /var/lib/deadline/queues

WORKDIR /home/agentuser

COPY --chown=agentuser:agentuser run_agent.sh /home/agentuser/
COPY --chown=agentuser:agentuser term_agent.sh /home/agentuser/

USER agentuser

CMD ["/bin/sh", "-c", "./run_agent.sh"]