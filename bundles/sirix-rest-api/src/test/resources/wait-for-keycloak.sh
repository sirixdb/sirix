#!/bin/bash

# Start Keycloak in the background
/opt/keycloak/bin/kc.sh "$@" &

# Wait for Keycloak to be ready
until curl -sf http://localhost:8080 > /dev/null; do
  echo "Waiting for Keycloak to be ready..."
  sleep 5
done

# Keycloak is ready, run the create-sirix-users.sh script
/opt/keycloak/scripts/create-sirix-users.sh

# Keep the container running
wait