# vertx-mqtt-server
This reactive provides a server which is able to handle connections, communication and messages exchange with remote MQTT clients.
# Introduction

This module is responsible for enabling communications with remote MQTT clients, which in the case of the SOFT-IoT platform are connected devices. In addition to the advantages of using a modular architecture, the use of the MQTT Vert.x API makes it possible to scale the reactive MQTT agent according to, for example, the number of cores in the system and thus enable horizontal scalability.

# Installation
Before installing the soft-iot-vertx-mqtt-broker module you need to introduce some Vert.x modules to ServiceMix. The dependencies are located at:

https://github.com/WiserUFBA/soft-iot-vertx-mqtt-broker/tree/master/src/main/resources/dependencies

In addition to installing the dependencies, it is also necessary to include files related to the soft-iot-vertx-mqtt-broker security certificate. These files are available at:

https://github.com/WiserUFBA/soft-iot-vertx-mqtt-broker/tree/master/src/main/resources/certificates

The security certificate has a configuration file available at:

https://github.com/WiserUFBA/soft-iot-vertx-mqtt-broker/blob/master/src/main/resources/configuration/br.ufba.dcc.wiser.soft_iot.gateway.brokers.cfg

The security certificate and configuration files should be stored in:

<diretorio_servicemix>/etc

# Deployment
For installation of soft-iot-vertx-mqtt-broker it is necessary to execute the following commands in the ServiceMix terminal:

karaf@root()> bundle:install mvn:br.ufba.dcc.wiser.soft_iot/soft-iot-vertx-mqtt-broker/1.0.0

