package io.github.giovibal.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;
import io.vertx.core.streams.Pump;
import org.dna.mqtt.moquette.proto.messages.ConnectMessage;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * Created by giova_000 on 29/06/2015.
 */
public class BridgeVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {

        JsonObject conf = config();
        String bridgeServerHost = conf.getString("bridge_server_host", "192.168.231.53");
        Integer bridgeServerPort = conf.getInteger("bridge_server_port", 1883);

        NetClientOptions opt = new NetClientOptions().setReconnectInterval(1000L);
        vertx.createNetClient(opt).connect(bridgeServerPort, bridgeServerHost, netSocketAsyncResult -> {
            if(netSocketAsyncResult.succeeded()) {
                NetSocket slaveBrokerSocket = netSocketAsyncResult.result();
                MQTTNetSocketWrapper slave = new MQTTNetSocketWrapper(slaveBrokerSocket);

                ConnectMessage connectMessage = new ConnectMessage();
                connectMessage.setClientID("mqtt-bridge-"+ UUID.randomUUID().toString());
                connectMessage.setCleanSession(true);
                slave.sendMessageToClient(connectMessage);

                //TODO: resend subscribe messages for all clients ...

                MessageConsumer<Buffer> consumer = vertx.eventBus().localConsumer(MQTTSession.ADDRESS);
                consumer.handler(bufferMessage -> {
                    slave.sendMessageToClient(bufferMessage.body());
                });
            }
        });

    }

}
