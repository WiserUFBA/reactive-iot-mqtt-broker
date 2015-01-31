package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.MQTTStoreManager;
import io.github.giovibal.mqtt.persistence.Subscription;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.dna.mqtt.moquette.proto.messages.SubscribeMessage;
import org.dna.mqtt.moquette.proto.messages.UnsubscribeMessage;

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

    public MQTTSession(Vertx vertx
            , MQTTSocket mqttSocket
            , String clientID, boolean cleanSession
            , String tenant) {

        this.vertx = vertx;
//        this.container = container;

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

    public String getClientID() {
        return clientID;
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
            Container.logger().error(e.getMessage());
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
//                    if (body instanceof JsonObject && mqttJson.isDeserializable((JsonObject) body)) {
//                        JsonObject json = (JsonObject) body;
//                        PublishMessage pm = mqttJson.deserializePublishMessage(json);
//                        // the qos is the max required ...
//                        QOSType originalQos = pm.getQos();
//                        int iSentQos = qosUtils.toInt(originalQos);
//                        int iOkQos = qosUtils.calculatePublishQos(iSentQos, iMaxQos);
//                        pm.setQos(qosUtils.toQos(iOkQos));
//                        pm.setRetainFlag(false);// server must send retain=false flag to subscribers ...
//                        mqttSocket.sendMessageToClient(pm);
//                    }
                    if (body instanceof Buffer) {
                        Buffer in = (Buffer)body;
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
                    Container.logger().error(e.getMessage(), e);
                }
            }
        };
//        Set<Handler<Message>> clientHandlers = getClientHandlers(topic);
//        clientHandlers.add(handler);
//        vertx.eventBus().registerHandler(toVertxTopic(topic), handler);
//        topicsManager.addSubscribedTopic(topic);
        MessageConsumer consumer = vertx.eventBus().localConsumer(toVertxTopic(topic));
//        MessageConsumer consumer = vertx.eventBus().consumer(toVertxTopic(topic));
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
                        getStore().deleteSubcription(topic, clientID);
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
            getStore().pushMessage(m, topicToPublish);
        } catch(Exception e) {
            Container.logger().error(e.getMessage(), e);
        }
    }
    private void storeAsLastMessage(PublishMessage publishMessage, String topicToPublish) {
        try {
            byte[] m = encoder.enc(publishMessage).getBytes();
            getStore().saveMessage(m, topicToPublish);
        } catch(Exception e) {
            Container.logger().error(e.getMessage(), e);
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
            Container.logger().error(e.getMessage(), e);
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
            Set<MessageConsumer> clientHandlers = getClientHandlers(topic);
            for (MessageConsumer messageConsumer : clientHandlers) {
//                vertx.eventBus().unregisterHandler(toVertxTopic(topic), handler);
                messageConsumer.unregister();
                topicsManager.removeSubscribedTopic(topic);
                if (clientID != null && cleanSession) {
                    getStore().deleteSubcription(topic, clientID);
                }
            }
        }
        mqttSocket = null;
        vertx = null;
    }
}
