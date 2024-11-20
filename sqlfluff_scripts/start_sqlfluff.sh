#!/bin/bash

# Navigate to the SQLFluff directory
cd /app/sqlfluff

# Set up the virtual environment using tox
tox -e py310 --devenv .venv

# Activate the virtual environment
source .venv/bin/activate

# Keep the container running by tailing a file indefinitely
tail -f /dev/null