#!/bin/bash

KEYCLOAK_HOME="/opt/jboss/keycloak"
REALM="sirixdb"
USERNAME="admin"
PASSWORD="admin"
ROLES="create,modify,delete,view"

user_exists() {
  $KEYCLOAK_HOME/bin/kcadm.sh get users -r "$REALM" -q "username=$USERNAME" --fields username | grep -q "\"username\" : \"$USERNAME\""
}

$KEYCLOAK_HOME/bin/kcadm.sh config credentials --server http://localhost:8080/auth --realm master --user "$USERNAME" --password "$PASSWORD"

if user_exists; then
  echo "User '$USERNAME' already exists in realm '$REALM'."
else
  if $KEYCLOAK_HOME/bin/add-user-keycloak.sh -r "$REALM" -u "$USERNAME" -p "$PASSWORD" --roles "$ROLES"; then
    echo "User '$USERNAME' added to realm '$REALM'."
  else
    echo "Failed to add user '$USERNAME' to realm '$REALM'."
    exit 1
  fi
fi