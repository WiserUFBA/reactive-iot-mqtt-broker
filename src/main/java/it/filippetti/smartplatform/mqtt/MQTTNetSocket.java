package it.filippetti.smartplatform.mqtt;

import org.dna.mqtt.moquette.proto.messages.ConnectMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.shareddata.ConcurrentSharedMap;
import org.vertx.java.platform.Container;

/**
 * Created by giovanni on 07/05/2014.
 */
public class MQTTNetSocket extends MQTTSocket {

    private NetSocket netSocket;
    private ConcurrentSharedMap<String, Buffer> messagesStore;
    private ConcurrentSharedMap<String, JsonObject> willMessagesStore;

    public MQTTNetSocket(Vertx vertx, Container container, NetSocket netSocket) {
        super(vertx, container);
        this.netSocket = netSocket;
        messagesStore = vertx.sharedData().getMap("messages");
        willMessagesStore = vertx.sharedData().getMap("will_messages");
    }

    public void start() {
        netSocket.dataHandler(this);
    }


    @Override
    protected void sendMessageToClient(Buffer bytes) {
        try {
            if (!netSocket.writeQueueFull()) {
                netSocket.write(bytes);
            } else {
                netSocket.pause();
                netSocket.drainHandler(new VoidHandler() {
                    public void handle() {
                        netSocket.resume();
                    }
                });
            }

        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }


//    @Override
//    protected void storeMessage(PublishMessage publishMessage) {
//        try {
//            Buffer tostore = encoder.enc(publishMessage);
//            String key = publishMessage.getTopicName();
//            messagesStore.put(key, tostore);
//        } catch(Throwable e) {
//            container.logger().error(e.getMessage());
//        }
//    }
//
//    @Override
//    protected void deleteMessage(PublishMessage publishMessage) {
//        try {
//            String key = publishMessage.getTopicName();
//            if(messagesStore.containsKey(key)) {
//                messagesStore.remove(key);
//            }
//        } catch(Throwable e) {
//            container.logger().error(e.getMessage());
//        }
//    }

    @Override
    protected void storeWillMessage(String willMsg, byte willQos, String willTopic) {
        JsonObject wm = mqttJson.serializeWillMessage(willMsg, willQos, willTopic);
        willMessagesStore.put(willTopic, wm);
    }
}
