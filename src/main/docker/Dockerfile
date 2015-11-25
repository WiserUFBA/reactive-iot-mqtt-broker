# A simple example showing how the vertx image can be used.

FROM java:8

WORKDIR /opt

# Copy resources
COPY vertx-mqtt-broker-mod-2.2-SNAPSHOT-fat.jar mqtt-broker.jar
COPY config.json config.json
COPY cluster.xml cluster.xml
COPY logging.properties logging.properties
COPY entrypoint.sh entrypoint.sh

RUN chmod +x entrypoint.sh

# Expose ports (5701 is hazelcast)
EXPOSE 1883 11883 1884 11884 1885 11885 7007 5701

# We use the "sh -c" to turn around https://github.com/docker/docker/issues/5509 - variable not expanded
ENTRYPOINT ["sh", "-c"]
#CMD ["java -Dhazelcast.ip=${IP} -Dhazelcast.public.address=${PUBLIC_ADDRESS} -jar mqtt-broker.jar -c config.json -hc cluster.xml -hh ${IP} -hm 192.168.231.52 192.168.231.51"]
CMD ["./entrypoint.sh", "${PUBLIC_ADDRESS}", "${CLUSTER_MEMBERS}"]
