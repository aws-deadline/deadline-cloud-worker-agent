#!/bin/sh
# Set the -e option
set -e

if command -v uv > /dev/null 2>&1; then
  uv pip install --upgrade hatch "virtualenv<21"
  uv pip install --upgrade twine
else
  pip install --upgrade pip
  pip install --upgrade hatch "virtualenv<21"
  pip install --upgrade twine
fi
hatch -v run lint
hatch -v run e2e:lint
hatch run test
hatch -v build