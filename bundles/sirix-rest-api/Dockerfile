###
# Sirix Vert.x based HTTP(S)-server backend packaged as a fatjar.
# To build (current dir is the sirix-rest-api module root):
#  docker build -t sirixdb/sirix .
# To run:
#  docker run -t -i -p 9443:9443 sirixdb/sirix
###

FROM openjdk:11.0.1-jre
MAINTAINER Johannes Lichtenberger <johannes.lichtenberger@sirix.io>

ENV VERTICLE_FILE sirix-rest-api-*-SNAPSHOT-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /opt/sirix

VOLUME $VERTICLE_HOME

EXPOSE 9443

# Copy your fat jar to the container
COPY target/$VERTICLE_FILE $VERTICLE_HOME/
COPY src/main/resources/cert.pem $VERTICLE_HOME/sirix-data
COPY src/main/resources/key.pem $VERTICLE_HOME/sirix-data
COPY src/main/resources/sirix-conf.json $VERTICLE_HOME/

# Launch the verticle
WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -jar -Duser.home=$VERTICLE_HOME $VERTICLE_FILE -conf sirix-conf.json -cp $VERTICLE_HOME/*"]
