#!/bin/sh
# Set the -e option
set -e

pip install --upgrade pip
pip install --upgrade hatch 'virtualenv<21; python_version < "3.10"'

if [ "$TEST_TYPE" ]
then
  if [ "$TEST_TYPE" = "WHEEL" ]
  then
    hatch build
    hatch env create
    export WORKER_AGENT_WHL_PATH=dist/`hatch run metadata name | sed 's/-/_/g'`-`hatch run version`-py3-none-any.whl
    echo "Set WORKER_AGENT_WHL_PATH to $WORKER_AGENT_WHL_PATH"
  fi
fi

# test/e2e/conftest.py pins every worker's session_runtime to this value when set.
echo "WORKER_AGENT_SESSION_RUNTIME=${WORKER_AGENT_SESSION_RUNTIME:-<unset, agent default>}"

hatch run e2e:test