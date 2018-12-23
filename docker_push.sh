#!/bin/bash
cd bundles/sirix-rest-api
docker build -t sirix ./src/main/docker
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker push sirixdb/sirix
