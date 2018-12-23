#!/bin/bash
cd bundles/sirix-rest-api
docker build -t sirix-rest-api -f src/main/docker/Dockerfile
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker push sirixdb/sirix
