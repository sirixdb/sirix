#!/bin/bash
docker-compose build --no-cache server
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker-compose push server
