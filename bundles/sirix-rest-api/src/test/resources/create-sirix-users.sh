#!/bin/bash

KEYCLOAK_HOME="/opt/keycloak"
REALM="sirixdb"
USERNAME="admin"
PASSWORD="admin"
ROLES="create,modify,delete,view"

user_exists() {
  $KEYCLOAK_HOME/bin/kcadm.sh get users -r "$REALM" -q "username=$USERNAME" --fields username | grep -q "\"username\" : \"$USERNAME\""
}

$KEYCLOAK_HOME/bin/kcadm.sh config credentials --server http://localhost:8080 --realm master --user "$USERNAME" --password "$PASSWORD"

if user_exists; then
  echo "User '$USERNAME' already exists in realm '$REALM'."
else
  if $KEYCLOAK_HOME/bin/kcadm.sh create users -r "$REALM" -s username="$USERNAME" -s enabled=true && \
     $KEYCLOAK_HOME/bin/kcadm.sh set-password -r "$REALM" --username "$USERNAME" --new-password "$PASSWORD" && \
     for role in $(echo $ROLES | tr "," "\n"); do
       $KEYCLOAK_HOME/bin/kcadm.sh add-roles -r "$REALM" --uusername "$USERNAME" --rolename "$role"
     done; then
    echo "User '$USERNAME' added to realm '$REALM'."
  else
    echo "Failed to add user '$USERNAME' to realm '$REALM'."
    exit 1
  fi
fi