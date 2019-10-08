# Stage-1
# Build jar

FROM maven:3-jdk-11 as builder
LABEL maintainer="Johannes Lichtenberger <johannes.lichtenberger@sirix.io>"
WORKDIR /usr/app

# Resolve dependencies
COPY pom.xml .
RUN ["/usr/local/bin/mvn-entrypoint.sh", "mvn", "verify", "clean", "--fail-never"]

# Package jar
COPY . .
RUN mvn package -DskipTests

# Stage-2
# Copy jar and run the server 

FROM openjdk:11-alpine as server
ENV VERTICLE_FILE sirix-rest-api-*-SNAPSHOT-fat.jar
# Set the location of the verticles
ENV VERTICLE_HOME /opt/sirix
WORKDIR /opt/sirix

# Copy fat jar to the container
COPY --from=builder /usr/app/target/$VERTICLE_FILE ./
COPY src/main/resources/cert.pem ./sirix-data/
COPY src/main/resources/key.pem ./sirix-data/
COPY src/main/resources/sirix-conf.json ./
# Replace localhost url with keycloak url in docker compose file
# RUN sed -i 's/localhost/keycloack/g' sirix-conf.json

VOLUME $VERTICLE_HOME
EXPOSE 9443

# Launch the verticle
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -jar -Duser.home=$VERTICLE_HOME $VERTICLE_FILE -conf sirix-conf.json -cp $VERTICLE_HOME/*"]
