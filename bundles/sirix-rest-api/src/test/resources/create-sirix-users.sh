#!/bin/bash

KEYCLOAK_HOME="/opt/keycloak"
REALM="sirixdb"
KCADM="$KEYCLOAK_HOME/bin/kcadm.sh"

$KCADM config credentials --server http://keycloak:8080 --realm master --user admin --password admin

# Create a user with the given username, password, and comma-separated roles.
create_user() {
  local username="$1"
  local password="$2"
  local roles="$3"

  if $KCADM get users -r "$REALM" -q "username=$username" --fields username | grep -q "\"username\" : \"$username\""; then
    echo "User '$username' already exists in realm '$REALM'."
    return 0
  fi

  if $KCADM create users -r "$REALM" -s username="$username" -s enabled=true && \
     $KCADM set-password -r "$REALM" --username "$username" --new-password "$password" && \
     for role in $(echo "$roles" | tr "," "\n"); do
       $KCADM add-roles -r "$REALM" --uusername "$username" --rolename "$role"
     done; then
    echo "User '$username' added to realm '$REALM'."
  else
    echo "Failed to add user '$username' to realm '$REALM'."
    exit 1
  fi
}

# Admin user: all roles
create_user "admin" "admin" "create,modify,delete,view"

# Viewer user: view-only (for authorization negative tests)
create_user "viewer" "viewer" "view"
