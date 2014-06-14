package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.MQTTStoreManager;
import io.github.giovibal.mqtt.persistence.Subscription;
import org.dna.mqtt.moquette.proto.messages.*;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

import java.util.*;

import static org.dna.mqtt.moquette.proto.messages.AbstractMessage.*;

/**
 * Created by giovanni on 07/05/2014.
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTSession {

    private Vertx vertx;
    private Container container;
    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    private MQTTJson mqttJson;
    private QOSUtils qosUtils;
    private Map<String, Set<Handler<Message>>> handlers;
    private MQTTTopicsManager topicsManager;
    private String clientID;
    private boolean cleanSession;
    private MQTTStoreManager store;
    private String tenant;
    private MQTTSocket mqttSocket;

    public MQTTSession(Vertx vertx, Container container
            , MQTTSocket mqttSocket
            , String clientID, boolean cleanSession
            , String tenant) {

        decoder = new MQTTDecoder();
        encoder = new MQTTEncoder();
        mqttJson = new MQTTJson();
        qosUtils = new QOSUtils();
        handlers = new HashMap<>();
//        tokenizer = new MQTTTokenizer();
//        tokenizer.registerListener(this);
//        topicsManager = new MQTTTopicsManager(vertx);
//        store = new MQTTStoreManager(vertx);

        this.vertx = vertx;
        this.container = container;
        this.mqttSocket = mqttSocket;
//        this.topicsManager = topicsManager;
        this.clientID = clientID;
        this.cleanSession = cleanSession;
        this.tenant = tenant;

        this.topicsManager = new MQTTTopicsManager(this.vertx, this.tenant);
        this.store = new MQTTStoreManager(this.vertx, this.tenant);
    }




    public void handlePublishMessage(PublishMessage publishMessage, boolean activatePersistence) {
        try {
            String topic = publishMessage.getTopicName();
            JsonObject msg = mqttJson.serializePublishMessage(publishMessage);

            Set<String> topicsToPublish = topicsManager.calculateTopicsToPublish(topic);
            QOSType qt = publishMessage.getQos();
            for (String tpub : topicsToPublish) {
                if(activatePersistence) {
                    if (qt == QOSType.EXACTLY_ONCE || qt == QOSType.LEAST_ONE) {
                        storeMessage(publishMessage, tpub);
                    }
                }
                vertx.eventBus().publish(toVertxTopic(tpub), msg);
            }
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }
    private String toVertxTopic(String mqttTopic) {
        String s = tenant +"/"+ mqttTopic;
        s = s.replaceAll("/+","/"); // remove multiple slashes
        return s;
    }

    private QOSType toQos(byte qosByte) {
        return new QOSUtils().toQos(qosByte);
    }

    public void handleSubscribeMessage(SubscribeMessage subscribeMessage) throws Exception {
        try {
            List<SubscribeMessage.Couple> subs = subscribeMessage.subscriptions();
            for (SubscribeMessage.Couple c : subs) {
                byte requestedQosByte = c.getQos();
                final QOSType requestedQos = toQos(requestedQosByte);
                String topic = c.getTopic();
                subscribeClientToTopic(topic, requestedQos);

                if(clientID!=null && cleanSession==false) {
                    Subscription s = new Subscription();
                    s.setQos(requestedQosByte);
                    s.setTopic(topic);
                    getStore().saveSubscription(s, clientID);
                }

                // replay saved messages
                republishPendingMessagesForSubscription(topic);
            }
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }

    private void republishPendingMessages() throws Exception {
        if(cleanSession) {
            // session is not persistent
        }
        else {
            // session is persistent...
            MQTTStoreManager store = getStore();
            List<Subscription> subscriptions = store.getSubscriptionsByClientID(clientID);
            for (Subscription sub : subscriptions) {
                // subsribe
                QOSType qos = new QOSUtils().toQos(sub.getQos());
                String topic2 = sub.getTopic();
                subscribeClientToTopic(topic2, qos);

                republishPendingMessagesForSubscription(topic2);
            }
        }
    }
    private void republishPendingMessagesForSubscription(String topic2) throws Exception {
        if(!cleanSession) {
            // session is persistent...
            MQTTStoreManager store = getStore();
            // subsribe

            // re-publish
            List<byte[]> messages = store.getMessagesByTopic(topic2, clientID);
            for(byte[] message : messages) {
                // publish message to this client
                PublishMessage pm = (PublishMessage)decoder.dec(new Buffer(message));
//                    handlePublishMessage(pm, false);
                // send message directly to THIS client
                mqttSocket.sendMessageToClient(pm);
                // delete will appen when publish end correctly.
                deleteMessage(pm);
            }
        }
    }

    private void subscribeClientToTopic(final String topic, QOSType requestedQos) {
        final int iMaxQos = qosUtils.toInt(requestedQos);
        Handler<Message> handler = new Handler<Message>() {
            @Override
            public void handle(Message message) {
                try {
                    JsonObject json = (JsonObject) message.body();
                    PublishMessage pm = mqttJson.deserializePublishMessage(json);
                    // the qos is the max required ...
                    QOSType originalQos = pm.getQos();
                    int iSentQos = qosUtils.toInt(originalQos);
                    int iOkQos = qosUtils.calculatePublishQos(iSentQos, iMaxQos);
                    pm.setQos(qosUtils.toQos(iOkQos));
                    pm.setRetainFlag(false);// server must send retain=false flag to subscribers ...

//                    if (cleanSession==false
//                            && isDisconnected()
//                            && (originalQos == QOSType.EXACTLY_ONCE || originalQos == QOSType.LEAST_ONE)) {
//                        storeMessage(pm, topic);
//                    }

                    mqttSocket.sendMessageToClient(pm);
                } catch (Throwable e) {
                    container.logger().error(e.getMessage(), e);
                }
            }
        };
        Set<Handler<Message>> clientHandlers = getClientHandlers(topic);
        clientHandlers.add(handler);
        vertx.eventBus().registerHandler(toVertxTopic(topic), handler);
        topicsManager.addSubscribedTopic(topic);
    }

    public void handleUnsubscribeMessage(UnsubscribeMessage unsubscribeMessage) {
        try {
            List<String> topics = unsubscribeMessage.topics();
            for (String topic : topics) {
                Set<Handler<Message>> clientHandlers = getClientHandlers(topic);
                for (Handler<Message> handler : clientHandlers) {
                    vertx.eventBus().unregisterHandler(toVertxTopic(topic), handler);
                    topicsManager.removeSubscribedTopic(topic);
                    // remove persistent subscriptions
                    if(clientID!=null/*&& cleanSession==false*/) {
                        getStore().deleteSubcription(topic, clientID);
                    }
                }
                clearClientHandlers(topic);
            }
        }
        catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }
    private Set<Handler<Message>> getClientHandlers(String topic) {
        String sessionID = topic;
        if(!handlers.containsKey(sessionID)) {
            handlers.put(sessionID, new HashSet<Handler<Message>>());
        }
        Set<Handler<Message>> clientHandlers = handlers.get(sessionID);
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
            getStore().pushMessage(m, topicToPublish);
        } catch(Exception e) {
            container.logger().error(e.getMessage(), e);
        }
    }
    private void deleteMessage(PublishMessage publishMessage) {
        try {
            String pubtopic = publishMessage.getTopicName();
            Set<String> topics = topicsManager.calculateTopicsToPublish(pubtopic);
            for(String tsub : topics) {
                getStore().popMessage(tsub, clientID);
            }
        } catch(Exception e) {
            container.logger().error(e.getMessage(), e);
        }
    }

    private MQTTStoreManager getStore() {
        return store;
    }

//    protected void addClientID(String clientID) {
//        Set<String> allClientIDs = vertx.sharedData().getSet("clientIDs");
//        allClientIDs.add(clientID);
//    }
//    protected boolean clientIDExists(String clientID) {
//        Set<String> allClientIDs = vertx.sharedData().getSet("clientIDs");
//        boolean exists = allClientIDs.contains(clientID);
//        return exists;
//    }
//
//    protected void removeClientID(String clientID) {
//        Set<String> allClientIDs = vertx.sharedData().getSet("clientIDs");
//        allClientIDs.remove(clientID);
//    }

//    private boolean isDisconnected() {
//        return clientID == null;
//    }

    public void shutdown() {
        //deallocate this instance ...
        Set<String> topics = handlers.keySet();
        for (String topic : topics) {
            Set<Handler<Message>> clientHandlers = getClientHandlers(topic);
            for (Handler<Message> handler : clientHandlers) {
                vertx.eventBus().unregisterHandler(toVertxTopic(topic), handler);
                topicsManager.removeSubscribedTopic(topic);
                if (clientID != null && cleanSession) {
                    getStore().deleteSubcription(topic, clientID);
                }
            }
//            clearClientHandlers(topic);
        }
//        removeClientID(clientID);
//        clientID = null;
    }
}
