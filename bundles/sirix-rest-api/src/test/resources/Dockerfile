# No ubi-micro-build stage: nothing in this image needs curl or jq any more.
#
# There used to be one that grafted them in from registry.access.redhat.com, and it was a silent
# trap. Its install ran as "RUN dnf install ... -y; dnf ... clean all" -- with a SEMICOLON, so the
# layer's exit status came from the trailing clean and a failed install was masked. Where the
# registry is reachable (GitHub Actions) curl landed and everything worked; behind a proxy that
# blocks it, the image built successfully WITHOUT curl, and the breakage only surfaced far
# downstream as a healthcheck that never went green and a Keycloak that looped through realm
# import until H2 gave up.
#
# create-sirix-users.sh uses only kcadm.sh, and both readiness probes now use bash's /dev/tcp,
# so the dependency is gone rather than merely repaired.
FROM quay.io/keycloak/keycloak:25.0.1


# Copy the necessary scripts into the container
COPY wait-for-keycloak.sh /opt/keycloak/scripts/wait-for-keycloak.sh
COPY create-sirix-users.sh /opt/keycloak/scripts/create-sirix-users.sh

USER root

RUN chmod +x /opt/keycloak/scripts/wait-for-keycloak.sh /opt/keycloak/scripts/create-sirix-users.sh

# Switch back to the default Keycloak user