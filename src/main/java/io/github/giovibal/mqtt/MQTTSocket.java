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
    protected String clientID;
    private boolean cleanSession;
    private MQTTStoreManager store;
    private String tenant;

    private MQTTSession session;

    public MQTTSocket(Vertx vertx, Container container) {
        decoder = new MQTTDecoder();
        encoder = new MQTTEncoder();
        mqttJson = new MQTTJson();
        qosUtils = new QOSUtils();
        handlers = new HashMap<>();
        tokenizer = new MQTTTokenizer();
        tokenizer.registerListener(this);
//        topicsManager = new MQTTTopicsManager(vertx);
//        store = new MQTTStoreManager(vertx/*, container*/);

        this.vertx = vertx;
        this.container = container;
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
                    session.handleSubscribeMessage(subscribeMessage);
                    SubAckMessage subAck = new SubAckMessage();
                    subAck.setMessageID(subscribeMessage.getMessageID());
//                    for(SubscribeMessage.Couple c : subscribeMessage.subscriptions()) {
//                        QOSType qos = toQos(c.getQos());
//                        subAck.addType(qos);
//                    }
//                    if(subscribeMessage.isRetainFlag()) {
//                        /*
//                        When a new subscription is established on a topic,
//                        the last retained message on that topic should be sent to the subscriber with the Retain flag set.
//                        If there is no retained message, nothing is sent
//                        */
//                    }
                    sendMessageToClient(subAck);
                    break;
                case UNSUBSCRIBE:
                    UnsubscribeMessage unsubscribeMessage = (UnsubscribeMessage)msg;
                    session.handleUnsubscribeMessage(unsubscribeMessage);
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
                            session.handlePublishMessage(publish, false);
                            break;
                        case LEAST_ONE:
                            // 1. Store message
                            // 2. publish message to subscribers
                            // 3. Delete message
                            // 4. <- PubAck
//                            storeMessage(publish);
                            session.handlePublishMessage(publish, true);
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
                            session.handlePublishMessage(publish, true);
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

                    PubCompMessage pubComp = new PubCompMessage();
                    pubComp.setMessageID(pubRel.getMessageID());
                    sendMessageToClient(pubComp);
                    break;
                case DISCONNECT:
                    // TODO:
                    // 1. terminate the session
                    // If flag "clean_session" of CONNECT was == 0, then remember the client subscriptions for next connection
                    DisconnectMessage disconnectMessage = (DisconnectMessage)msg;
                    handleDisconnect(disconnectMessage);
                    break;
                case PUBACK:
                    // A PUBACK message is the response to a PUBLISH message with QoS level 1.
                    // A PUBACK message is sent by a server in response to a PUBLISH message from a publishing client,
                    // and by a subscriber in response to a PUBLISH message from the server.
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



    public void sendMessageToClient(AbstractMessage message) {
        try {
            Buffer b1 = encoder.enc(message);
            sendMessageToClient(b1);
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }
    abstract protected void sendMessageToClient(Buffer bytes);

    private String getTenant(ConnectMessage connectMessage) {
        String tenant = "";
        String clientID = connectMessage.getClientID();
        int idx = clientID.lastIndexOf('@');
        if(idx > 0) {
            tenant = clientID.substring(idx+1);
        }
        return tenant;
    }

    private void handleConnectMessage(ConnectMessage connectMessage) throws Exception {
        ConnectMessage connect = connectMessage;

        clientID = connect.getClientID();
        cleanSession = connect.isCleanSession();

        // initialize tenant
        tenant = getTenant(connectMessage);

        topicsManager = new MQTTTopicsManager(vertx, tenant);
        store = new MQTTStoreManager(vertx, tenant);


        boolean clientIDExists = clientIDExists(clientID);
        container.logger().info("Connect ClientID ==> "+ clientID);
        if(clientIDExists) {
            // TODO: reset older clientID socket connection
            container.logger().info("Connect ClientID ==> "+ clientID +" alredy exists !!");
        } else {
            addClientID(clientID);
        }

        if(connect.isWillFlag()) {
            String willMsg = connect.getWillMessage();
            byte willQos = connect.getWillQos();
            String willTopic = connect.getWillTopic();
            storeWillMessage(willMsg, willQos, willTopic);
        }


        // TODO: instanziate MQTTSession and to all the logic there.
        // in case of cleanSession=false, session instance must persist (with HashMap reference ?)
        session = new MQTTSession(vertx, container, this, clientID, cleanSession, tenant);

    }


    private void handleDisconnect(DisconnectMessage disconnectMessage) {
        session.shutdown();
    }


    protected void storeWillMessage(String willMsg, byte willQos, String willTopic) {

    }

    private void addClientID(String clientID) {
        Set<String> allClientIDs = vertx.sharedData().getSet("clientIDs");
        allClientIDs.add(clientID);
    }
    private boolean clientIDExists(String clientID) {
        Set<String> allClientIDs = vertx.sharedData().getSet("clientIDs");
        boolean exists = allClientIDs.contains(clientID);
        return exists;
    }

    private void removeClientID(String clientID) {
        Set<String> allClientIDs = vertx.sharedData().getSet("clientIDs");
        allClientIDs.remove(clientID);
    }

}