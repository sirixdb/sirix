#!/bin/bash
cd bundles/sirix-rest-api
mvn clean package -DskipTests
docker build -t sirixdb/sirix .
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker push sirixdb/sirix
