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
* Multi-tenancy: isolation of topics and storage, just use client@tenant as ClientID


Roadmap
----
* Expose persistence SPI interfaces
* Implement SPI for login management
* Implement some out-of-the-box persistence plugins: RAM, Cassandra, HBase, MongoDB
* Implement will message support 
* Make more tests
