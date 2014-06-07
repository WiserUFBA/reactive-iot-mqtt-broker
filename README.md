vertx-mqtt-broker-mod
=====================

MQTT broker implementation as Vert.x module.

Credits:
<br/>
Moquette <a href="https://code.google.com/p/moquette-mqtt/">https://code.google.com/p/moquette-mqtt/</a>
for coder and decoder implementation of MQTT messages.
<br/>


Quick Start
-----------
Required: Vert.x 2.1M1+ and Maven 3+

```
git clone https://github.com/giovibal/vertx-mqtt-broker-mod.git
cd vertx-mqtt-broker-mod
mvn clean package
mvn vertx:runMod
```
or if you have vert.x installed ...
```
mvn clean install
vertx runmod io.gihub.giovibal.mqtt~vertx-mqtt-broker-mod~1.0-SNAPSHOT
```

Features
----
* Suport both QoS 1 and 2 messages
* Persistence and session management (cleanSession=false)
* TODO - Expose persistence SPI interfaces
* TODO - Implement SPI for login management
* TODO - Implement multi-tenancy
* TODO - Implement some out-of-the-box persistence plugins: RAM/Hazelcast, Cassandra, HBase, MongoDB
* TODO - Implement will message support 
* TODO - Make more tests
