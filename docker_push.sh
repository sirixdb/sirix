#!/bin/bash
cd bundles/sirix-rest-api
man clean package -DskipTests 
docker build -t sirixdb/sirix ./src/main/docker
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker push sirixdb/sirix
