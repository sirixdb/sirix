#!/bin/bash
#
# Entrypoint: start Keycloak, wait until it actually serves, then seed the test users.
#
# This deliberately uses NOTHING but bash. The Keycloak image ships no package manager and no
# curl -- the Dockerfile's ubi-micro-build stage is supposed to graft one in, but the binary does
# not survive into the final image ("command -v curl" finds nothing at runtime). The previous
# "until curl -sf ..." loop therefore spun forever on exit 127: create-sirix-users.sh never ran,
# so no test user ever existed, and the compose healthcheck (also curl) never went healthy, so
# composeUp timed out. With "restart: always" the container then relooped through realm import
# until H2 fell over with "Connection is closed" -- which looked like a Keycloak bug and was not.
#
# bash's /dev/tcp is enough to speak just enough HTTP to confirm the realm endpoint answers.

set -uo pipefail

KC_HOST="${KC_PROBE_HOST:-localhost}"
KC_PORT="${KC_PROBE_PORT:-8080}"
KC_PROBE_PATH="${KC_PROBE_PATH:-/realms/master}"
READY_TIMEOUT_SECONDS="${KC_READY_TIMEOUT:-300}"

# The compose healthcheck waits for this marker, NOT merely for Keycloak to answer. Keycloak
# serving /realms/master only means the server is up; the test users do not exist until
# create-sirix-users.sh below has run. Gating "healthy" on liveness alone lets composeUp return
# while seeding is still in flight, and the suite then fails with "invalid_grant: Invalid user
# credentials" -- a race that reads like a broken password.
READY_MARKER="${KC_READY_MARKER:-/tmp/keycloak-seeded}"
rm -f "${READY_MARKER}"

# Start Keycloak in the background.
/opt/keycloak/bin/kc.sh "$@" &
KC_PID=$!

# Answers 0 only when the endpoint returns a 2xx/3xx status line.
probe_keycloak() {
  exec 3<>"/dev/tcp/${KC_HOST}/${KC_PORT}" 2>/dev/null || return 1
  printf 'GET %s HTTP/1.1\r\nHost: %s:%s\r\nConnection: close\r\n\r\n' \
      "${KC_PROBE_PATH}" "${KC_HOST}" "${KC_PORT}" >&3 2>/dev/null || { exec 3<&-; return 1; }
  local status_line=""
  read -r status_line <&3 2>/dev/null
  exec 3<&- 2>/dev/null
  exec 3>&- 2>/dev/null
  case "${status_line}" in
    *" 200 "*|*" 301 "*|*" 302 "*|*" 303 "*|*" 307 "*|*" 308 "*) return 0 ;;
    *) return 1 ;;
  esac
}

echo "Waiting for Keycloak on ${KC_HOST}:${KC_PORT}${KC_PROBE_PATH} (timeout ${READY_TIMEOUT_SECONDS}s)..."
deadline=$(( SECONDS + READY_TIMEOUT_SECONDS ))
until probe_keycloak; do
  # Fail loudly instead of spinning forever: a dead Keycloak used to be indistinguishable from a
  # slow one, which is exactly how the curl-not-found bug stayed hidden.
  if ! kill -0 "${KC_PID}" 2>/dev/null; then
    echo "ERROR: Keycloak exited before becoming ready." >&2
    wait "${KC_PID}"
    exit 1
  fi
  if (( SECONDS >= deadline )); then
    echo "ERROR: Keycloak did not become ready within ${READY_TIMEOUT_SECONDS}s." >&2
    exit 1
  fi
  sleep 5
done

echo "Keycloak is ready; seeding test users."
if ! /opt/keycloak/scripts/create-sirix-users.sh; then
  echo "ERROR: seeding test users failed." >&2
  exit 1
fi

# Only now is the container genuinely usable by the suite.
touch "${READY_MARKER}"
echo "Test users seeded; container is ready."

# Hand the container's lifetime back to Keycloak.
wait "${KC_PID}"
