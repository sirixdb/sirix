FROM registry.access.redhat.com/ubi9 AS ubi-micro-build
RUN mkdir -p /mnt/rootfs
RUN dnf install --installroot /mnt/rootfs jq curl \
    --releasever 9 --setopt install_weak_deps=false --nodocs -y; \
    dnf --installroot /mnt/rootfs clean all

FROM quay.io/keycloak/keycloak:25.0.1

# Copy dependencies from ubi-micro-build stage
COPY --from=ubi-micro-build /mnt/rootfs /


# Copy the necessary scripts into the container
COPY wait-for-keycloak.sh /opt/keycloak/scripts/wait-for-keycloak.sh
COPY create-sirix-users.sh /opt/keycloak/scripts/create-sirix-users.sh

USER root

RUN chmod +x /opt/keycloak/scripts/wait-for-keycloak.sh /opt/keycloak/scripts/create-sirix-users.sh

# Switch back to the default Keycloak user