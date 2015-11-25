#!/bin/bash

IP=$(ip addr | grep inet | grep eth0 | awk -F" " '{print $2}' | sed -e 's/\/.*$//')

echo "IP: ${IP}, PUBLIC_ADDRESS: ${PUBLIC_ADDRESS}, CLUSTER_MEMBERS: ${CLUSTER_MEMBERS}"

java -XX:+UseG1GC -XX:G1ReservePercent=50 -Djava.util.logging.config.file=logging.properties -Dhazelcast.ip=${IP} -Dhazelcast.public.address=${PUBLIC_ADDRESS} -jar mqtt-broker.jar -c config.json -hc cluster.xml -hh ${IP} -hm "${CLUSTER_MEMBERS}"
