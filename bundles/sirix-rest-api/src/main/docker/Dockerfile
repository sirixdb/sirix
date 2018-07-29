###
# vert.x docker example using a Java verticle packaged as a fatjar
# To build:
#  docker build -t sirix-rest-api .
# To run:
#  docker run -t -i -p 8443:8443 sirix-rest-api
###

FROM java:9-jre

ENV VERTICLE_FILE sirix-rest-api-0.9.0-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

EXPOSE 8443

# Copy your fat jar to the container
COPY target/$VERTICLE_FILE $VERTICLE_HOME/

# Launch the verticle
WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -jar $VERTICLE_FILE"]