package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.MQTTStoreManager;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.LocalMap;
import org.dna.mqtt.moquette.proto.messages.*;

import static org.dna.mqtt.moquette.proto.messages.AbstractMessage.*;

/**
 * Created by giovanni on 07/05/2014.
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public abstract class MQTTSocket implements MQTTTokenizer.MqttTokenizerListener, Handler<Buffer> {

    protected Vertx vertx;
//    protected Container container;
    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    protected MQTTTokenizer tokenizer;
    private MQTTTopicsManager topicsManager;
    private String clientID;
    private boolean cleanSession;
    private MQTTStoreManager store;
    private String tenant;
    private MQTTSession session;

    public MQTTSocket(Vertx vertx) {
        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.tokenizer = new MQTTTokenizer();
        this.tokenizer.registerListener(this);

        this.vertx = vertx;
    }


    public void shutdown() {
        if(tokenizer!=null) {
            tokenizer.removeAllListeners();
            tokenizer = null;
        }
        if(session!=null) {
            session.shutdown();
            session = null;
        }
        vertx = null;
    }



    @Override
    public void onToken(byte[] token, boolean timeout) {
        Buffer buffer = Buffer.buffer(token);
        try {
            AbstractMessage message = decoder.dec(buffer);
            onMessageFromClient(message);
        }
        catch (Exception e) {
//            container.logger().error(e.getMessage(), e);
        }
    }

    @Override
    public void handle(Buffer buffer) {
//        System.out.println("handle "+ buffer.length());
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
                    for(SubscribeMessage.Couple c : subscribeMessage.subscriptions()) {
                        QOSType qos = new QOSUtils().toQos(c.getQos());
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
                            session.handlePublishMessage(publish);
                            break;
                        case LEAST_ONE:
                            session.handlePublishMessage(publish);
                            PubAckMessage pubAck = new PubAckMessage();
                            pubAck.setMessageID(publish.getMessageID());
                            sendMessageToClient(pubAck);
                            break;
                        case EXACTLY_ONCE:
                            session.handlePublishMessage(publish);
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
                    PubCompMessage pubComp = new PubCompMessage();
                    pubComp.setMessageID(pubRel.getMessageID());
                    sendMessageToClient(pubComp);
                    break;
                case DISCONNECT:
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
//                    container.logger().warn("type of message not known: "+ msg.getClass().getSimpleName());
                    break;
            }
        } catch (Exception ex) {
            Container.logger().error("Bad error in processing the message", ex);
        }
    }


    public void sendMessageToClient(AbstractMessage message) {
        try {
            Buffer b1 = encoder.enc(message);
            sendMessageToClient(b1);
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
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
//        container.logger().info("Connect ClientID ==> "+ clientID);
        if(clientIDExists) {
            // Resume old session
            Container.logger().info("Connect ClientID ==> "+ clientID +" alredy exists !!");
        } else {
            addClientID(clientID);
        }

        session = new MQTTSession(vertx, this, clientID, cleanSession, tenant);

        if(connect.isWillFlag()) {
            String willMsg = connect.getWillMessage();
            byte willQos = connect.getWillQos();
            String willTopic = connect.getWillTopic();
            session.storeWillMessage(willMsg, willQos, willTopic);
        }
    }

    private void handleDisconnect(DisconnectMessage disconnectMessage) {
        removeClientID(clientID);
        session.shutdown();
        session = null;
    }

    private void addClientID(String clientID) {
        LocalMap<String, Object> allClientIDs = vertx.sharedData().getLocalMap("clientIDs");
        allClientIDs.put(clientID, new Object());
    }
    private boolean clientIDExists(String clientID) {
        LocalMap<String, Object> allClientIDs = vertx.sharedData().getLocalMap("clientIDs");
        boolean exists = allClientIDs.keySet().contains(clientID);
        return exists;
    }

    private void removeClientID(String clientID) {
        LocalMap<String, Object> allClientIDs = vertx.sharedData().getLocalMap("clientIDs");
        allClientIDs.remove(clientID);
    }

}
