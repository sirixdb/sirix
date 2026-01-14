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
ENV VERTICLE_FILE sirix-rest-api-*-SNAPSHOT-fat.jar
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

# Launch the verticle
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -DLOGGER_HOME=/opt/sirix/sirix-data -Xms4g -Xmx16g -XX:MaxDirectMemorySize=4g --enable-preview --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector -Ddisable.single.threaded.check -XX:+UseZGC -XX:+HeapDumpOnOutOfMemoryError -XX:+UseStringDeduplication -XX:+AlwaysPreTouch --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED -jar -Duser.home=$VERTICLE_HOME $VERTICLE_FILE -conf sirix-docker-conf.json -cp $VERTICLE_HOME/*"]
