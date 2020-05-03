# Stage-1
# Build jar

FROM gradle:6.3.0-jdk14 as builder
LABEL maintainer="Johannes Lichtenberger <johannes.lichtenberger@sirix.io>"
WORKDIR /usr/app/

# Package jar
COPY . .
RUN gradle build --refresh-dependencies -x test

# Stage-2
# Copy jar and run the server 

FROM openjdk:14-alpine as server
RUN apk update && apk add --no-cache bash && apk add --no-cache gcompat
ENV VERTICLE_FILE sirix-rest-api-*-SNAPSHOT-fat.jar
# Set the location of the verticles
ENV VERTICLE_HOME /opt/sirix
WORKDIR /opt/sirix

# Copy fat jar to the container
COPY --from=builder /usr/app/bundles/sirix-rest-api/build/libs/$VERTICLE_FILE ./

# Copy additional configuration files
COPY bundles/sirix-rest-api/src/main/resources/cert.pem ./sirix-data/
COPY bundles/sirix-rest-api/src/main/resources/key.pem ./sirix-data/
COPY bundles/sirix-rest-api/src/main/resources/sirix-conf.json ./

# Replace localhost url with keycloack url in docker compose file
# RUN sed -i 's/localhost/keycloak/g' sirix-conf.json

VOLUME $VERTICLE_HOME
EXPOSE 9443

# Launch the verticle
ENTRYPOINT ["sh", "-c"]
CMD ["exec java --enable-preview -jar -Duser.home=$VERTICLE_HOME $VERTICLE_FILE -conf sirix-conf.json -cp $VERTICLE_HOME/*"]
