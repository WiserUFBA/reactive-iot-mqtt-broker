package it.filippetti.smartplatform.mqtt;

import it.filippetti.smartplatform.mqtt.parser.MQTTDecoder;
import it.filippetti.smartplatform.mqtt.parser.MQTTEncoder;
import it.filippetti.smartplatform.mqtt.persistence.MQTTStoreManager;
import it.filippetti.smartplatform.mqtt.persistence.Subscription;
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
 */
public abstract class MQTTSocket implements MQTTTokenizer.MqttTokenizerListener, Handler<Buffer> {

    protected Vertx vertx;
    protected Container container;
    protected MQTTDecoder decoder;
    protected MQTTEncoder encoder;
    protected MQTTJson mqttJson;
    private QOSUtils qosUtils;

    private Map<String, Set<Handler<Message>>> handlers;
    private MQTTTokenizer tokenizer;
    private MQTTTopicsManager topicsManager;

    private String clientID;
    private boolean cleanSession;
    private PublishMessage lastPublishMessage;

    public MQTTSocket(Vertx vertx, Container container) {
        decoder = new MQTTDecoder();
        encoder = new MQTTEncoder();
        mqttJson = new MQTTJson();
        qosUtils = new QOSUtils();
        handlers = new HashMap<>();
        tokenizer = new MQTTTokenizer();
        tokenizer.registerListener(this);
        topicsManager = new MQTTTopicsManager(vertx);

        this.vertx = vertx;
        this.container = container;

        this.container.logger().info("New " + this.getClass().getSimpleName() + " " + this);
    }


    @Override
    public void onToken(byte[] token, boolean timeout) {
        Buffer buffer = new Buffer(token);
        try {
            AbstractMessage message = decoder.dec(buffer);
            onMessageFromClient(message);
        }
        catch (Exception e) {
            container.logger().error(e.getMessage(), e);
        }
    }

    @Override
    public void handle(Buffer buffer) {
        tokenizer.process(buffer.getBytes());
    }


    protected void onMessageFromClient(AbstractMessage msg) {
        try {
            switch (msg.getMessageType()) {
                case CONNECT:
                    ConnectMessage connect = (ConnectMessage)msg;
                    handleConnectMessage(connect);
                    ConnAckMessage connAck = new ConnAckMessage();
                    sendMessageToClient(connAck);
                    break;
                case SUBSCRIBE:
                    SubscribeMessage subscribeMessage = (SubscribeMessage)msg;
                    handleSubscribeMessage(subscribeMessage);
                    SubAckMessage subAck = new SubAckMessage();
                    subAck.setMessageID(subscribeMessage.getMessageID());
                    for(SubscribeMessage.Couple c : subscribeMessage.subscriptions()) {
                        QOSType qos = toQos(c.getQos());
                        subAck.addType(qos);
                    }
                    if(subscribeMessage.isRetainFlag()) {
                        /*
                         When a new subscription is established on a topic,
                         the last retained message on that topic should be sent to the subscriber with the Retain flag set.
                         If there is no retained message, nothing is sent
                         */
                    }
                    sendMessageToClient(subAck);
                    break;
                case UNSUBSCRIBE:
                    UnsubscribeMessage unsubscribeMessage = (UnsubscribeMessage)msg;
                    handleUnsubscribeMessage(unsubscribeMessage);
                    UnsubAckMessage unsubAck = new UnsubAckMessage();
                    unsubAck.setMessageID(unsubscribeMessage.getMessageID());
                    sendMessageToClient(unsubAck);
                    break;
                case PUBLISH:
                    PublishMessage publish = (PublishMessage)msg;
                    switch (publish.getQos()) {
                        case RESERVED:
                            break;
                        case MOST_ONE:
                            // 1. publish message to subscribers
                            handlePublishMessage(publish, false);
                            break;
                        case LEAST_ONE:
                            // 1. Store message
                            // 2. publish message to subscribers
                            // 3. Delete message
                            // 4. <- PubAck
//                            storeMessage(publish);
                            handlePublishMessage(publish, true);
//                            deleteMessage(publish);
                            PubAckMessage pubAck = new PubAckMessage();
                            pubAck.setMessageID(publish.getMessageID());
                            sendMessageToClient(pubAck);
                            break;

                        case EXACTLY_ONCE:
                            // 1. Store message
                            // 2. publish message to subscribers
                            // 3. <- PubRec
                            // 4. -> PubRel from client
                            // 5. Delete message
                            // 5. <- PubComp
//                            storeMessage(publish);
                            handlePublishMessage(publish, true);
                            PubRecMessage pubRec = new PubRecMessage();
                            pubRec.setMessageID(publish.getMessageID());
                            sendMessageToClient(pubRec);
                            break;
                    }
                    break;
                case PUBREC:
                    PubRecMessage pubRec = (PubRecMessage)msg;
                    PubRelMessage prelResp = new PubRelMessage();
                    prelResp.setMessageID(pubRec.getMessageID());
                    sendMessageToClient(prelResp);
                    break;
                case PUBCOMP:
//                    deleteMessage();
                    break;
                case PUBREL:
                    PubRelMessage pubRel = (PubRelMessage)msg;
                    // HISTORY:
                    // 1. Store message
                    // 2. publish message to subscrribers
                    // 3. <- PubRec
                    // ------> 4. -> PubRel from client
                    // TODO:
                    // 5. Delete message
                    // 5. <- PubComp

//                    deleteMessage();
                    PubCompMessage pubComp = new PubCompMessage();
                    pubComp.setMessageID(pubRel.getMessageID());
                    sendMessageToClient(pubComp);
                    break;
                case DISCONNECT:
                    // TODO:
                    // 1. terminate the session
                    // se il flag "clean_session" del CONNECT era == 0, allora non pulisce le subscriptions di questo client
                    removeClientID(clientID);
                    clientID = null;
                    break;
                case PUBACK:
                    // A PUBACK message is the response to a PUBLISH message with QoS level 1.
                    // A PUBACK message is sent by a server in response to a PUBLISH message from a publishing client,
                    // and by a subscriber in response to a PUBLISH message from the server.
//                    deleteMessage();
                    break;
                case PINGREQ:
                    PingRespMessage pingResp = new PingRespMessage();
                    sendMessageToClient(pingResp);
                    break;
                default:
                    container.logger().warn("type of message not known: "+ msg.getClass().getSimpleName());
                    break;
            }
        } catch (Exception ex) {
            container.logger().error("Bad error in processing the message", ex);
        }
    }



    protected void sendMessageToClient(AbstractMessage message) {
        try {
            Buffer b1 = encoder.enc(message);
            sendMessageToClient(b1);
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }

    protected void handleConnectMessage(ConnectMessage connectMessage) throws Exception {
        ConnectMessage connect = connectMessage;
        clientID = connect.getClientID();
        cleanSession = connect.isCleanSession();
        boolean clientIDExists = clientIDExists(clientID);
        container.logger().info("Connect ClientID ==> "+ clientID);
        if(clientIDExists) {
            // TODO: reset older clientID socket connection
            container.logger().info("Connect ClientID ==> "+ clientID +" alredy exists !!");
        }
        container.logger().info(clientID + " " + this);
//        if(cleanSession) {
//            // session is not persistent
//        }
//        else {
//            // session is persistent...
//            MQTTStoreManager store = getStore();
//            List<Subscription> subscriptions = store.getSubscriptionsByClientID(clientID);
//            for (Subscription sub : subscriptions) {
//                // subsribe
//                QOSType qos = new QOSUtils().toQos(sub.getQos());
//                String topic = sub.getTopic();
//                subscribeClientToTopic(topic, qos);
//
//                // re-publish
//                List<byte[]> messages = store.getMessagesByTopic(topic);
//                for(byte[] message : messages) {
//                    // publish message to this client
//                    PublishMessage pm = (PublishMessage)decoder.dec(new Buffer(message));
//                    handlePublishMessage(pm, false);
//                    // delete will appen when publish end correctly.
//                    deleteMessage(pm);
//                }
//            }
//        }
        republishMessages();

        if(connect.isWillFlag()) {
            String willMsg = connect.getWillMessage();
            byte willQos = connect.getWillQos();
            String willTopic = connect.getWillTopic();
            storeWillMessage(willMsg, willQos, willTopic);
        }
    }


    protected void handlePublishMessage(PublishMessage publishMessage, boolean activatePersistence) {
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
                vertx.eventBus().publish(tpub, msg);
            }
            lastPublishMessage = publishMessage;
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }

    private QOSType toQos(byte qosByte) {
        return new QOSUtils().toQos(qosByte);
    }

    protected void handleSubscribeMessage(SubscribeMessage subscribeMessage) throws Exception {
        try {
            List<SubscribeMessage.Couple> subs = subscribeMessage.subscriptions();
            for (SubscribeMessage.Couple c : subs) {
                byte requestedQosByte = c.getQos();
                final QOSType requestedQos = toQos(requestedQosByte);
//                final int iMaxQos = qosUtils.toInt(requestedQos);
                String topic = c.getTopic();

                subscribeClientToTopic(topic, requestedQos);

//                republishMessages();


                if(clientID!=null && cleanSession==false) {
                    Subscription s = new Subscription();
                    s.setQos(requestedQosByte);
                    s.setTopic(topic);
                    getStore().saveSubscription(s, clientID);
                }
            }
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }

    private void republishMessages() throws Exception {
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

                // re-publish
                List<byte[]> messages = store.getMessagesByTopic(topic2);
                for(byte[] message : messages) {
                    // publish message to this client
                    PublishMessage pm = (PublishMessage)decoder.dec(new Buffer(message));
                    handlePublishMessage(pm, false);
                    // delete will appen when publish end correctly.
                    deleteMessage(pm);
                }
            }
        }
    }

    protected void subscribeClientToTopic(String topic, QOSType requestedQos) {
        final int iMaxQos = qosUtils.toInt(requestedQos);
        Handler<Message> handler = new Handler<Message>() {
            @Override
            public void handle(Message message) {
                try {
                    JsonObject json = (JsonObject) message.body();
                    PublishMessage pm = mqttJson.deserializePublishMessage(json);
                    // il qos è quello MASSIMO RICHIESTO
                    int iSentQos = qosUtils.toInt(pm.getQos());
                    int iOkQos = qosUtils.calculatePublishQos(iSentQos, iMaxQos);
                    pm.setQos(qosUtils.toQos(iOkQos));
                    pm.setRetainFlag(false);// server must send retain=false flag to subscribers ...
                    sendMessageToClient(pm);
                } catch (Throwable e) {
                    container.logger().error(e.getMessage(), e);
                }
            }
        };
        Set<Handler<Message>> clientHandlers = getClientHandlers(topic);
        clientHandlers.add(handler);
        vertx.eventBus().registerHandler(topic, handler);
        topicsManager.addSubscribedTopic(topic);
    }

    protected void handleUnsubscribeMessage(UnsubscribeMessage unsubscribeMessage) {
        try {
            List<String> topics = unsubscribeMessage.topics();
            for (String topic : topics) {
                Set<Handler<Message>> clientHandlers = getClientHandlers(topic);
                for (Handler<Message> handler : clientHandlers) {
                    vertx.eventBus().unregisterHandler(topic, handler);
                    topicsManager.removeSubscribedTopic(topic);
                    if(clientID!=null && cleanSession==false) {
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


    abstract protected void sendMessageToClient(Buffer bytes);
    protected void storeMessage(PublishMessage publishMessage, String topicToPublish) {
//        lastPublishMessage = publishMessage;
        try {
            byte[] m = encoder.enc(publishMessage).getBytes();
            String key = clientID+publishMessage.getMessageID();
            getStore().saveMessage(key, m, topicToPublish);
        } catch(Exception e) {
            container.logger().error(e.getMessage(), e);
        }
    }
    protected void deleteMessage(PublishMessage publishMessage) {
        try {
            byte[] m = encoder.enc(publishMessage).getBytes();
            String key = clientID+publishMessage.getMessageID();
            Set<String> topics = topicsManager.calculateTopicsToPublish(publishMessage.getTopicName());
            for(String tsub : topics) {
                getStore().deleteMessage(key, m, tsub);
            }
        } catch(Exception e) {
            container.logger().error(e.getMessage(), e);
        }
//        lastPublishMessage = null;
    }
    protected void deleteMessage() {
//        if(lastPublishMessage!=null)
        // for now, let throws NullPointerException ...
            deleteMessage(lastPublishMessage);
    }
    abstract protected void storeWillMessage(String willMsg, byte willQos, String willTopic);


    protected MQTTStoreManager getStore() {
        MQTTStoreManager s = new MQTTStoreManager(vertx, container);
        return s;
    }

    protected boolean clientIDExists(String clientID) {
        Set<String> allClientIDs = vertx.sharedData().getSet("clientIDs");
        boolean exists = allClientIDs.contains(clientID);
        if(exists) {
            return false;
        } else {
            allClientIDs.add(clientID);
            return true;
        }
    }

    protected void removeClientID(String clientID) {
        Set<String> allClientIDs = vertx.sharedData().getSet("clientIDs");
        allClientIDs.remove(clientID);
    }
}
