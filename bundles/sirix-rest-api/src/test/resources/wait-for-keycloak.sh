#!/bin/bash

# Start Keycloak in the background
/opt/keycloak/bin/kc.sh "$@" &

# Determine if running in GitHub Actions or locally
if [ "$GITHUB_ACTIONS" == "true" ]; then
  KEYCLOAK_HOST="keycloak"
else
  KEYCLOAK_HOST="localhost"
fi

# Wait for Keycloak to be ready
until curl -sf http://$KEYCLOAK_HOST:8080 > /dev/null; do
  echo "Waiting for Keycloak to be ready..."
  sleep 5
done

# Keycloak is ready, run the create-sirix-users.sh script
/opt/keycloak/scripts/create-sirix-users.sh

# Keep the container running
wait
