#!/bin/sh
# Set the -e option
set -e

pip install --upgrade pip
pip install --upgrade hatch

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

if [ "$OPERATING_SYSTEM" = "linux" ]
  then
    hatch run linux-e2e-test 
fi

if [ "$OPERATING_SYSTEM" = "windows" ]
  then
    hatch run windows-e2e-test
fi

hatch run cross-os-e2e-test