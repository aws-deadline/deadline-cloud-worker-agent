#!/bin/sh
# Set the -e option
set -e

pip install --upgrade pip
pip install --upgrade hatch

if [ $TEST_TYPE ]
then
  if [ $TEST_TYPE = "WHEEL" ]
  then
    hatch run codebuild:build
    export WORKER_AGENT_WHL_PATH=dist/`hatch run codebuild\:metadata name | sed 's/-/_/g'`-`hatch run codebuild\:version`-py3-none-any.whl
    echo "Set WORKER_AGENT_WHL_PATH to $WORKER_AGENT_WHL_PATH"
  fi
else
  continue
fi

hatch run codebuild:integ-test