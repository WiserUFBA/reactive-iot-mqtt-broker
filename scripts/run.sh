#!/usr/bin/env bash
java -jar -Xmx2g -Xms2g -XX:+UseG1GC -XX:G1ReservePercent=50 -jar target\vertx-mqtt-broker-mod-2.2-SNAPSHOT-fat.jar -c config-2.0.json