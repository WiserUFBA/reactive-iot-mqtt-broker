# A simple example showing how the vertx image can be used.

FROM vertx/vertx3:3.0.0

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

# Set the name of the verticle to deploy
ENV VERTICLE_NAME io.github.giovibal.mqtt.MQTTBroker

# Set vertx option
ENV VERTX_OPTIONS "-conf $VERTICLE_HOME/config.json"

# Set Vertx/Java Opts
#ENV VERTX_OPTS="-agentlib:jdwp=transport=dt_socket,address=8000,suspend=n,server=y"
ENV VERTX_OPTS=""

# Copy resources
COPY vertx-mqtt-broker-mod-2.0-SNAPSHOT-fat.jar $VERTICLE_HOME/mqtt-broker.jar
COPY config.json $VERTICLE_HOME/config.json

# Expose ports
EXPOSE 1883 11883
EXPOSE 1884 11884
EXPOSE 1885 11885
EXPOSE 7007
#EXPOSE 8000


# We use the "sh -c" to turn around https://github.com/docker/docker/issues/5509 - variable not expanded
ENTRYPOINT ["sh", "-c"]
CMD ["vertx run $VERTICLE_NAME -instances 1 -cp $VERTICLE_HOME/*.jar $VERTX_OPTIONS"]