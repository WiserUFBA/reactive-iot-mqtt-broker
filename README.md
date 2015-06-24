vertx-mqtt-broker-mod
=====================

MQTT broker implementation as Vert.x module.

Credits:
<br/>
Moquette <a href="https://github.com/andsel/moquette">https://github.com/andsel/moquette</a>
for coder and decoder implementation of MQTT messages.
<br/>


Quick Start
-----------
Requires Vert.x 3.0.0 and Maven 3+

```
git clone https://github.com/giovibal/vertx-mqtt-broker-mod.git
cd vertx-mqtt-broker-mod
mvn clean install
```
use vertx command to start the service ...
```
vertx run maven:io.github.giovibal.mqtt:vertx-mqtt-broker-mod:2.0-SNAPSHOT::mqtt-broker -conf config.json
vertx run maven:io.github.giovibal.mqtt:vertx-mqtt-broker-mod:2.0-SNAPSHOT::mqtt-broker -Dvertx.metrics.options.jmxEnabled=true -conf config.json
```
or uber jar ...
```
java -jar target/vertx-mqtt-broker-mod-2.0-SNAPSHOT-fat.jar -conf config.json
```

cluster ...
```
vertx run maven:io.github.giovibal.mqtt:vertx-mqtt-broker-mod:2.0-SNAPSHOT::mqtt-broker -conf config.json -cluster -cluster-host 192.168.1.188
```

Features
----
* Suport both QoS 0, 1 and 2 messages
* Persistence and session management (cleanSession=false)
* Multi-tenancy: isolation of topics and storage, just use username@tenant as username
* MQTT over WebSocket
* Retain flag
* Oauth2 authentication integrated with <a href="http://wso2.com/products/identity-server/">WSO2 Identity Server</a>
* TLS support over TCP and Websocket
* Multiple endpoint configuration in the same broker instance  

Work in progress
----
* Refactoring, new architecture
* Clustering and Hign Availability

Roadmap
----
* Pluggable persistence 
* Implement some out-of-the-box persistence managers: RAM, Cassandra, HBase, MongoDB
* Implement will message support 
