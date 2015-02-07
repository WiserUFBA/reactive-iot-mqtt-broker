package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.MQTTStoreManager;
import io.github.giovibal.mqtt.persistence.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.*;
import io.vertx.core.json.JsonObject;
import org.dna.mqtt.moquette.proto.messages.*;

import java.nio.ByteBuffer;
import java.util.*;

import static org.dna.mqtt.moquette.proto.messages.AbstractMessage.QOSType;

/**
 * Created by giovanni on 07/05/2014.
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTSession {

    private Vertx vertx;
//    private Container container;
    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    private MQTTJson mqttJson;
    private QOSUtils qosUtils;
    private Map<String, Set<MessageConsumer>> handlers;
    private MQTTTopicsManager topicsManager;
    private String clientID;
    private boolean cleanSession;
    private MQTTStoreManager store;
    private String tenant;
    private MQTTSocket mqttSocket;

    public MQTTSession(Vertx vertx, MQTTSocket mqttSocket) {

        this.vertx = vertx;
//        this.container = container;

        this.mqttSocket = mqttSocket;
//        this.clientID = clientID;
//        this.cleanSession = cleanSession;
//        this.tenant = tenant;

        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.mqttJson = new MQTTJson();
        this.qosUtils = new QOSUtils();
        this.handlers = new HashMap<>();

//        this.topicsManager = new MQTTTopicsManager(this.vertx, this.tenant);
//        this.store = new MQTTStoreManager(this.vertx, this.tenant);
//
//        // save clientID
//        boolean clientIDExists = store.clientIDExists(this.clientID);
//        if(clientIDExists) {
//            // Resume old session
//            Container.logger().info("Connect ClientID ==> "+ clientID +" alredy exists !!");
//        } else {
//            store.addClientID(clientID);
//        }
    }

    public String getClientID() {
        return clientID;
    }

    private String getTenant(ConnectMessage connectMessage) {
        String tenant = "";
        String clientID = connectMessage.getClientID();
        int idx = clientID.lastIndexOf('@');
        if(idx > 0) {
            tenant = clientID.substring(idx+1);
        }
        return tenant;
    }

    public void handleConnectMessage(ConnectMessage connectMessage) throws Exception {
        clientID = connectMessage.getClientID();
        cleanSession = connectMessage.isCleanSession();
        tenant = getTenant(connectMessage);

        // AUTHENTICATION START
//        String username = connectMessage.getUsername();
//        String password = connectMessage.getPassword();
//
//        String address = MQTTBroker.class.getName() + "_auth";
//        JsonObject credentials = new JsonObject().put("username", username).put("password", password);
//        MessageProducer<JsonObject> producer = vertx.eventBus().sender(address);
//        producer.write(credentials);
//
//        vertx.eventBus().send(address, credentials, (AsyncResult<Message<JsonObject>> messageAsyncResult) -> {
//            if(messageAsyncResult.succeeded()) {
//                JsonObject reply = messageAsyncResult.result().body();
//                System.out.println(reply.toString());
//                Boolean authenticated = reply.getBoolean("authenticated");
//                System.out.println("authenticated ===> "+ authenticated );
//            }
//        });
        // AUTHENTICATION END

        topicsManager = new MQTTTopicsManager(this.vertx, this.tenant);
        store = new MQTTStoreManager(this.vertx, this.tenant);

        // save clientID
        boolean clientIDExists = store.clientIDExists(this.clientID);
        if(clientIDExists) {
            // Resume old session
            Container.logger().info("Connect ClientID ==> "+ clientID +" alredy exists !!");
        } else {
            store.addClientID(clientID);
        }

        if(connectMessage.isWillFlag()) {
            String willMsg = connectMessage.getWillMessage();
            byte willQos = connectMessage.getWillQos();
            String willTopic = connectMessage.getWillTopic();
            storeWillMessage(willMsg, willQos, willTopic);
        }
    }

    public void handleDisconnect(DisconnectMessage disconnectMessage) {
        shutdown();
    }

    public void handlePublishMessage(PublishMessage publishMessage) {
        try {
            String topic = publishMessage.getTopicName();
//            JsonObject msg = mqttJson.serializePublishMessage(publishMessage);
            Buffer msg = encoder.enc(publishMessage);
            Set<String> topicsToPublish = topicsManager.calculateTopicsToPublish(topic);

            // retain
            if(publishMessage.isRetainFlag()) {
                storeAsLastMessage(publishMessage, topic);
            }
            // QoS 1,2
            for (String tpub : topicsToPublish) {
                switch (publishMessage.getQos()) {
                    case LEAST_ONE:
                    case EXACTLY_ONCE:
                        storeMessage(publishMessage, tpub);
                        break;
                }
                vertx.eventBus().publish(toVertxTopic(tpub), msg);
            }
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }
    private String toVertxTopic(String mqttTopic) {
        return topicsManager.toVertxTopic(mqttTopic);
    }

    public void handleSubscribeMessage(SubscribeMessage subscribeMessage) throws Exception {
        try {
            List<SubscribeMessage.Couple> subs = subscribeMessage.subscriptions();
            for (SubscribeMessage.Couple c : subs) {
                byte requestedQosByte = c.getQos();
                final QOSType requestedQos = qosUtils.toQos(requestedQosByte);
                String topic = c.getTopicFilter();
                subscribeClientToTopic(topic, requestedQos);

                if(clientID!=null && !cleanSession) {
                    Subscription s = new Subscription();
                    s.setQos(requestedQosByte);
                    s.setTopic(topic);
                    store.saveSubscription(s, clientID);
                }

                // replay saved messages
                republishPendingMessagesForSubscription(topic);
            }
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    private void republishPendingMessagesForSubscription(String topic) throws Exception {
        // re-publish
        List<byte[]> messages = store.getMessagesByTopic(topic, clientID);
        for(byte[] message : messages) {
            // publish message to this client
            PublishMessage pm = (PublishMessage)decoder.dec(Buffer.buffer(message));
            // send message directly to THIS client
            // check if message topic matches topicFilter of subscription
            boolean ok = topicsManager.match(pm.getTopicName(), topic);
            if(ok) {
                mqttSocket.sendMessageToClient(pm);
            }
            // delete will happen when publish end correctly.
            deleteMessage(pm);
        }
    }

    private void subscribeClientToTopic(final String topic, QOSType requestedQos) {
        final int iMaxQos = qosUtils.toInt(requestedQos);
        Handler<Message<Buffer>> handler = new Handler<Message<Buffer>>() {
            @Override
            public void handle(Message<Buffer> message) {
                try {
//                    Object body = message.body();
//                    if (body instanceof Buffer) {
//                        Buffer in = (Buffer)body;
                        Buffer in = message.body();
                        PublishMessage pm = (PublishMessage)decoder.dec(in);
                        /* the qos is the max required ... */
                        QOSType originalQos = pm.getQos();
                        int iSentQos = qosUtils.toInt(originalQos);
                        int iOkQos = qosUtils.calculatePublishQos(iSentQos, iMaxQos);
                        pm.setQos(qosUtils.toQos(iOkQos));
                        /* server must send retain=false flag to subscribers ...*/
                        pm.setRetainFlag(false);
                        mqttSocket.sendMessageToClient(pm);
//                        mqttSocket.sendMessageToClient(in);
//                    }
//                    else {
//                        PublishMessage pm = new PublishMessage();
//                        pm.setTopicName(topic);
//                        pm.setPayload(ByteBuffer.wrap(message.body().toString().getBytes()));
//                        pm.setQos(QOSType.MOST_ONE);
//                        pm.setRetainFlag(false);// server must send retain=false flag to subscribers ...
//                        pm.setDupFlag(false);
//                        mqttSocket.sendMessageToClient(pm);
//                    }
                } catch (Throwable e) {
                    Container.logger().error(e.getMessage(), e);
                }
            }
        };
        MessageConsumer<Buffer> consumer = vertx.eventBus().localConsumer(toVertxTopic(topic));
        consumer.handler(handler);
        Set<MessageConsumer> messageConsumers = getClientHandlers(topic);
        messageConsumers.add(consumer);
        topicsManager.addSubscribedTopic(topic);
    }

    public void handleUnsubscribeMessage(UnsubscribeMessage unsubscribeMessage) {
        try {
            List<String> topics = unsubscribeMessage.topicFilters();
            for (String topic : topics) {
                Set<MessageConsumer> clientHandlers = getClientHandlers(topic);
                for (MessageConsumer messageConsumer : clientHandlers) {
//                    vertx.eventBus().unregisterHandler(toVertxTopic(topic), handler);
                    messageConsumer.unregister();
                    topicsManager.removeSubscribedTopic(topic);
                    // remove persistent subscriptions
                    if(clientID!=null && cleanSession) {
                        store.deleteSubcription(topic, clientID);
                    }
                }
                clearClientHandlers(topic);
            }
        }
        catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }
    private Set<MessageConsumer> getClientHandlers(String topic) {
        String sessionID = topic;
        if(!handlers.containsKey(sessionID)) {
            handlers.put(sessionID, new HashSet<>());
        }
        Set<MessageConsumer> clientHandlers = handlers.get(sessionID);
        return clientHandlers;
    }
    private void clearClientHandlers(String topic) {
        String sessionID = topic;
        if (handlers.containsKey(sessionID)) {
            handlers.remove(sessionID);
        }
    }


    private void storeMessage(PublishMessage publishMessage, String topicToPublish) {
        try {
            byte[] m = encoder.enc(publishMessage).getBytes();
            store.pushMessage(m, topicToPublish);
        } catch(Exception e) {
            Container.logger().error(e.getMessage(), e);
        }
    }
    private void storeAsLastMessage(PublishMessage publishMessage, String topicToPublish) {
        try {
            byte[] m = encoder.enc(publishMessage).getBytes();
            if(m.length == 0) {
                // remove retain message because payload is 0 length
                store.deleteMessage(topicToPublish);
            } else {
                store.saveMessage(m, topicToPublish);
            }
        } catch(Exception e) {
            Container.logger().error(e.getMessage(), e);
        }
    }

    private void deleteMessage(PublishMessage publishMessage) {
        try {
            String pubtopic = publishMessage.getTopicName();
            Set<String> topics = topicsManager.calculateTopicsToPublish(pubtopic);
            for(String tsub : topics) {
                store.popMessage(tsub, clientID);
            }
        } catch(Exception e) {
            Container.logger().error(e.getMessage(), e);
        }
    }


//    private MQTTStoreManager getStore() {
//        return store;
//    }

    public void storeWillMessage(String willMsg, byte willQos, String willTopic) {
        store.storeWillMessage(willMsg, willQos, willTopic);
    }

    public void shutdown() {
        //deallocate this instance ...
        Set<String> topics = handlers.keySet();
        for (String topic : topics) {
            Set<MessageConsumer> clientHandlers = getClientHandlers(topic);
            for (MessageConsumer messageConsumer : clientHandlers) {
//                vertx.eventBus().unregisterHandler(toVertxTopic(topic), handler);
                messageConsumer.unregister();
                topicsManager.removeSubscribedTopic(topic);
                if (clientID != null && cleanSession) {
                    store.deleteSubcription(topic, clientID);
                }
            }
        }
        store.removeClientID(clientID);
        mqttSocket = null;
        vertx = null;
    }
}
