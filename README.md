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
cd target
vertx runzip vertx-mqtt-broker-mod-1.0-SNAPSHOT-mod.zip
```

Features
----
* Suport both QoS 1 and 2 messages
* Persistence and session management (cleanSession=false)
* TODO - Clean persistence related code, make it extensible (RAM, Cassandra, HBase, MongoDB as plug-able storage engine)
* TODO - Implement will message support and all protocol features.
