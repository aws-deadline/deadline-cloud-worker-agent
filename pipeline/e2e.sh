#!/bin/sh
# Set the -e option
set -e

if command -v uv > /dev/null 2>&1; then
  uv pip install --upgrade hatch "virtualenv<21"
else
  pip install --upgrade pip
  pip install --upgrade hatch "virtualenv<21"
fi

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

hatch run e2e:test