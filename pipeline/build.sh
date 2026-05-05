#!/bin/sh
# Set the -e option
set -e

pip install --upgrade pip
pip install --upgrade hatch "virtualenv<21"
pip install --upgrade twine
hatch -v run lint
hatch -v run e2e:lint
hatch run test
hatch -v build