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

mvn clean package<br/>
cd target<br/>
vertx runzip vertx-mqtt-broker-mod-1.0-SNAPSHOT-mod.zip<br/>

TODO
----
<ul>
<li>Include tests
</li>
<li>Debug persistence og qos 1 and 2 messages, in case more client are connected and subscribed to the same topics
</li>
<li>Clean persistence related code, make it extensible (RAM, Cassandra, HBase, MongoDB as plug-able storage engine)
</li>
<li>Implement will message support and all protocol features.
</li>
</ul>