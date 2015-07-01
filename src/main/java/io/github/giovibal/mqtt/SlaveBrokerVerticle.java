package io.github.giovibal.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.net.*;
import io.vertx.core.streams.Pump;
import org.dna.mqtt.moquette.proto.messages.ConnectMessage;

import java.util.function.Consumer;

/**
 * Created by giova_000 on 29/06/2015.
 */
public class SlaveBrokerVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {
        NetClientOptions opt = new NetClientOptions().setReconnectInterval(1000L);
        vertx.createNetClient(opt).connect(1884, "192.168.231.53", netSocketAsyncResult -> {
            if(netSocketAsyncResult.succeeded()) {
                NetSocket slaveBrokerSocket = netSocketAsyncResult.result();
                MQTTNetSocketWrapper slave = new MQTTNetSocketWrapper(slaveBrokerSocket);

                ConnectMessage connectMessage = new ConnectMessage();
                connectMessage.setClientID("mqtt-broker-1");
                connectMessage.setCleanSession(true);
//                connectMessage.setKeepAlive(5);
                slave.sendMessageToClient(connectMessage);

                MessageConsumer<Buffer> consumer = vertx.eventBus().localConsumer(MQTTSession.ADDRESS);
//                consumer.setMaxBufferedMessages(10000);
                consumer.handler(bufferMessage -> {
                    slave.sendMessageToClient(bufferMessage.body());
                });
            }
        });

    }

}
