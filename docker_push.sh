#!/bin/bash
cd bundles/sirix-rest-api
mvn clean package -DskipTests
docker-compose build
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker-compose push