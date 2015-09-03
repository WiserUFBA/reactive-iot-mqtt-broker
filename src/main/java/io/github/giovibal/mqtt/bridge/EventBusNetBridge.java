package io.github.giovibal.mqtt.bridge;

import io.github.giovibal.mqtt.MQTTNetSocketWrapper;
import io.github.giovibal.mqtt.NetSocketWrapper;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.streams.Pump;

/**
 * Created by giova_000 on 15/07/2015.
 */
public class EventBusNetBridge {
    private static final String BR_HEADER = "bridged";

//    private MessageConsumer<Buffer> consumer;
//    private MessageProducer<Buffer> producer;
    private NetSocket netSocket;
    private EventBus eventBus;
    private String eventBusAddress;

    public EventBusNetBridge(NetSocket netSocket, EventBus eventBus, String eventBusAddress) {
        this.eventBus = eventBus;
        this.netSocket = netSocket;
        this.eventBusAddress = eventBusAddress;
    }

    public void start() {

        MessageConsumer<Buffer> consumer = eventBus.localConsumer(eventBusAddress);
        MessageProducer<Buffer> producer = eventBus.publisher(eventBusAddress, new DeliveryOptions().addHeader(BR_HEADER, BR_HEADER));

        // from remote tcp to local bus
        Pump.pump(netSocket, producer).start();

        // from local bus to remote tcp
        NetSocketWrapper netSocketWrapper = new MQTTNetSocketWrapper(netSocket);
        consumer.handler(bufferMessage -> {
            // filter bridged messages to prevent LOOP
            boolean isBridged = bufferMessage.headers() != null && bufferMessage.headers().contains(BR_HEADER);
            if (!isBridged) {
                netSocketWrapper.sendMessageToClient(bufferMessage.body());
            }
        });
    }
}
