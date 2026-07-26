# Stage-1
# Build jar
FROM gradle:jdk25 AS builder
LABEL maintainer="Johannes Lichtenberger <johannes.lichtenberger@sirix.io>"
WORKDIR /usr/app/

# Package jar
COPY . .
RUN gradle build --refresh-dependencies -x test -x javadoc

# Stage-2
# Copy jar and run the server
# Using glibc-based image for proper mmap support with Java FFM API

FROM eclipse-temurin:25-jre AS server
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
# Match the fat jar for ANY project version. A `*-SNAPSHOT-fat.jar` glob silently
# matched nothing for real releases (e.g. 1.0.0-alpha10): the COPY below copied no
# jar and the runtime `-jar` glob resolved to a missing file, yielding a jar-less
# image that fails with "Unable to access jarfile". `*-fat.jar` covers snapshots and
# releases alike (exactly one *-fat.jar artifact exists per build).
ENV VERTICLE_FILE sirix-rest-api-*-fat.jar
# Set the location of the verticles
ENV VERTICLE_HOME /opt/sirix
WORKDIR /opt/sirix

# Copy fat jar to the container
COPY --from=builder /usr/app/bundles/sirix-rest-api/build/libs/$VERTICLE_FILE ./

# Copy additional configuration files
RUN mkdir -p ./sirix-data/
COPY bundles/sirix-rest-api/src/test/resources/logback-test.xml ./sirix-data/
COPY bundles/sirix-rest-api/src/main/resources/cert.pem ./sirix-data/
COPY bundles/sirix-rest-api/src/main/resources/key.pem ./sirix-data/
COPY bundles/sirix-rest-api/src/main/resources/sirix-docker-conf.json ./

# Replace localhost url with keycloack url in docker compose file
# RUN sed -i 's/localhost/keycloak/g' sirix-conf.json

# Note: Don't use VOLUME here - it creates an empty volume that overlays the JAR
# VOLUME $VERTICLE_HOME
EXPOSE 9443

# Launch the verticle.
# Heap/direct-memory sizing is overridable via env vars so the default
# `docker compose up` runs on a laptop. Raise these for production/benchmarks,
# e.g. -e SIRIX_XMX=16g -e SIRIX_MAX_DIRECT=4g (and bump the compose memory limits).
# Extra JVM flags (cache budgets, GC tuning, system properties) can be appended
# via SIRIX_JAVA_OPTS, e.g. -e SIRIX_JAVA_OPTS="-Dsirix.cache.page=512m".
# SIRIX_AUTH_MODE=none starts the server without Keycloak (development only —
# see docs/QUICKSTART.md); the default is Keycloak-based OAuth2.
#
# Removed flags (do not cargo-cult them back):
# - `-Ddisable.single.threaded.check` was a no-op: the property belongs to the
#   Chronicle-Bytes library, which is not a SirixDB dependency on the release
#   line. Nothing on the classpath reads it.
# - `-XX:+AlwaysPreTouch` committed the entire initial heap as RSS at startup,
#   which hurts laptops/small VMs and buys nothing at a 256m -Xms.
ENV SIRIX_XMS=256m
ENV SIRIX_XMX=2g
ENV SIRIX_MAX_DIRECT=2g
ENV SIRIX_JAVA_OPTS=""
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -DLOGGER_HOME=/opt/sirix/sirix-data -Duser.home=/opt/sirix -Xms$SIRIX_XMS -Xmx$SIRIX_XMX -XX:MaxDirectMemorySize=$SIRIX_MAX_DIRECT --enable-preview --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector -XX:+UseZGC -XX:+HeapDumpOnOutOfMemoryError -XX:+UseStringDeduplication --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED $SIRIX_JAVA_OPTS -jar $VERTICLE_HOME/$VERTICLE_FILE -conf sirix-docker-conf.json"]
