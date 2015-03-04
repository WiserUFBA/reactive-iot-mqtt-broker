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
vertx run service:io.github.giovibal.mqtt:vertx-mqtt-broker-mod:2.0-SNAPSHOT -conf config.json
```
or uber jar ...
```
java -jar target\vertx-mqtt-broker-mod-2.0-SNAPSHOT-fat.jar -conf config.json
```

Features
----
* Suport both QoS 0, 1 and 2 messages
* Persistence and session management (cleanSession=false)
* Multi-tenancy: isolation of topics and storage, just use client@tenant as ClientID
* MQTT over WebSocket
* Retain flag

Roadmap
----
* Authentication with multi-tenancy association
* Expose persistence SPI interfaces
* Implement some out-of-the-box persistence managers: RAM, Cassandra, HBase, MongoDB
* Implement will message support 
