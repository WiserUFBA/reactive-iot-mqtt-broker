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

import java.nio.ByteBuffer;
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

        this.vertx = vertx;
        this.container = container;

        this.mqttSocket = mqttSocket;
        this.clientID = clientID;
        this.cleanSession = cleanSession;
        this.tenant = tenant;

        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.mqttJson = new MQTTJson();
        this.qosUtils = new QOSUtils();
        this.handlers = new HashMap<>();

        this.topicsManager = new MQTTTopicsManager(this.vertx, this.tenant);
        this.store = new MQTTStoreManager(this.vertx, this.tenant);
    }


    public void handlePublishMessage(PublishMessage publishMessage) {
        try {
            String topic = publishMessage.getTopicName();
            JsonObject msg = mqttJson.serializePublishMessage(publishMessage);
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
            container.logger().error(e.getMessage());
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

    private void republishPendingMessagesForSubscription(String topic) throws Exception {
//        if(!cleanSession) {
            // session is persistent...
            MQTTStoreManager store = getStore();
            // subsribe

            // re-publish
            List<byte[]> messages = store.getMessagesByTopic(topic, clientID);
            for(byte[] message : messages) {
                // publish message to this client
                PublishMessage pm = (PublishMessage)decoder.dec(new Buffer(message));
                // send message directly to THIS client
                // check if message topic matches topicFilter of subscription
                boolean ok = topicsManager.match(pm.getTopicName(), topic);
                if(ok) {
                    mqttSocket.sendMessageToClient(pm);
                }
                // delete will appen when publish end correctly.
                deleteMessage(pm);
            }
//        }
    }

    private void subscribeClientToTopic(final String topic, QOSType requestedQos) {
        final int iMaxQos = qosUtils.toInt(requestedQos);
        Handler<Message> handler = new Handler<Message>() {
            @Override
            public void handle(Message message) {
                try {
//                    JsonObject json = (JsonObject) message.body();
                    // Check if message is json and deserializable, otherwise try to construct a custom PublishMessage.
                    // Design from Paolo Iddas
                    Object body = message.body();
                    if (body instanceof JsonObject && mqttJson.isDeserializable((JsonObject) body)) {
                        JsonObject json = (JsonObject) body;
                        PublishMessage pm = mqttJson.deserializePublishMessage(json);
                        // the qos is the max required ...
                        QOSType originalQos = pm.getQos();
                        int iSentQos = qosUtils.toInt(originalQos);
                        int iOkQos = qosUtils.calculatePublishQos(iSentQos, iMaxQos);
                        pm.setQos(qosUtils.toQos(iOkQos));
                        pm.setRetainFlag(false);// server must send retain=false flag to subscribers ...
                        mqttSocket.sendMessageToClient(pm);
                    }
                    else {
                        PublishMessage pm = new PublishMessage();
                        pm.setTopicName(topic);
                        pm.setPayload(ByteBuffer.wrap(message.body().toString().getBytes()));
                        pm.setQos(QOSType.MOST_ONE);
                        pm.setRetainFlag(false);// server must send retain=false flag to subscribers ...
                        pm.setDupFlag(false);
                        mqttSocket.sendMessageToClient(pm);
                    }

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
            List<String> topics = unsubscribeMessage.topicFilters();
            for (String topic : topics) {
                Set<Handler<Message>> clientHandlers = getClientHandlers(topic);
                for (Handler<Message> handler : clientHandlers) {
                    vertx.eventBus().unregisterHandler(toVertxTopic(topic), handler);
                    topicsManager.removeSubscribedTopic(topic);
                    // remove persistent subscriptions
                    if(clientID!=null && cleanSession) {
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
    private void storeAsLastMessage(PublishMessage publishMessage, String topicToPublish) {
        try {
            byte[] m = encoder.enc(publishMessage).getBytes();
            getStore().saveMessage(m, topicToPublish);
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
//                getStore().deleteMessage(tsub);
            }
        } catch(Exception e) {
            container.logger().error(e.getMessage(), e);
        }
    }


    private MQTTStoreManager getStore() {
        return store;
    }

    public void storeWillMessage(String willMsg, byte willQos, String willTopic) {
        getStore().storeWillMessage(willMsg,willQos,willTopic);
    }

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
        mqttSocket = null;
        vertx = null;
        container = null;
    }
}
