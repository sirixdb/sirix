#!/usr/bin/env bash
cd bin
./kcadm.sh config credentials --server http://localhost:8080/auth --realm sirixdb --user admin --password admin

USERID=$(./kcadm.sh create users -r sirixdb -s username=admin -s enabled=true -o --fields id | jq '.id' | tr -d '"')
echo $USERID
./kcadm.sh update users/$USERID/reset-password -r sirixdb -s type=password -s value=default -s temporary=false -n
./kcadm.sh add-roles --uusername admin --rolename create --rolename view --rolename update --rolename delete -r sirixdb