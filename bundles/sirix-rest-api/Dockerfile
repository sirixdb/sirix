###
# Sirix Vert.x based HTTP(S)-server backend packaged as a fatjar.
# To build (current dir is the sirix-rest-api module root):
#  docker build -t sirix-rest-api .
# To run:
#  docker run -t -i -p 9443:9443 sirix-rest-api
###

FROM openjdk:9-jre

ENV VERTICLE_FILE sirix-rest-api-*-SNAPSHOT-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /opt/sirix

VOLUME $VERTICLE_HOME

EXPOSE 9443

# Copy your fat jar to the container
COPY target/$VERTICLE_FILE $VERTICLE_HOME/
COPY src/main/resources/ $VERTICLE_HOME/

# Launch the verticle
WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -jar -Duser.home=$VERTICLE_HOME -conf sirix-conf.json $VERTICLE_FILE"]