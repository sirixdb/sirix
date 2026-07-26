#!/bin/bash
set -euo pipefail
# Authentication is handled by the docker/login-action step in the workflow;
# this script only builds and pushes the server image.
docker compose build --no-cache server
docker compose push server
