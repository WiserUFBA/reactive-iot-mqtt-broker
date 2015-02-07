package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.MQTTStoreManager;
import io.netty.handler.codec.CorruptedFrameException;
import io.vertx.core.AsyncResult;
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
public abstract class MQTTSocket implements MQTTPacketTokenizer.MqttTokenizerListener, Handler<Buffer> {

    protected Vertx vertx;
//    protected Container container;
    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    protected MQTTPacketTokenizer tokenizer;
//    private MQTTTopicsManager topicsManager;
//    private String clientID;
//    private boolean cleanSession;
//    private MQTTStoreManager store;
//    private String tenant;
    private MQTTSession session;

    public MQTTSocket(Vertx vertx) {
        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.tokenizer = new MQTTPacketTokenizer();
        this.tokenizer.registerListener(this);

        this.vertx = vertx;
    }
    abstract protected void sendMessageToClient(Buffer bytes);
    abstract protected void closeConnection();


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
    public void handle(Buffer buffer) {
        tokenizer.process(buffer.getBytes());
    }

    @Override
    public void onToken(byte[] token, boolean timeout) throws Exception {
        Buffer buffer = Buffer.buffer(token);
//        try {
            AbstractMessage message = decoder.dec(buffer);
            onMessageFromClient(message);
//        }
//        catch (Exception e) {
//            Container.logger().error(e.getMessage(), e);
//        }
    }

    @Override
    public void onError(Throwable e) {
//        e.printStackTrace();
        Container.logger().error(e.getMessage(), e);
        if(e instanceof CorruptedFrameException) {
            closeConnection();
        }
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
                    if(subscribeMessage.getQos() != QOSType.LEAST_ONE) {
                        System.out.println("SUBSCRIBE QoS != 1 !!");
                    }
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
                    if(pubRec.getQos() != QOSType.MOST_ONE) {
                        System.out.println("PUBREC QoS != 0 !!");
                    }
                    PubRelMessage prelResp = new PubRelMessage();
                    prelResp.setMessageID(pubRec.getMessageID());
                    prelResp.setQos(QOSType.LEAST_ONE);
                    sendMessageToClient(prelResp);
                    break;
                case PUBCOMP:
                    break;
                case PUBREL:
                    PubRelMessage pubRel = (PubRelMessage)msg;
                    if(pubRel.getQos() != QOSType.LEAST_ONE) {
                        System.out.println("PUBREL QoS != 1 !!");
                    }
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
                    PubAckMessage pubAck = (PubAckMessage)msg;
                    if(pubAck.getQos() != QOSType.MOST_ONE) {
                        System.out.println("PUBACK QoS != 0 !!");
                    }
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
//        ConnectMessage connect = connectMessage;

//        String clientID = connect.getClientID();
//        boolean cleanSession = connect.isCleanSession();
//
//        // initialize tenant
//        String tenant = getTenant(connectMessage);

//        topicsManager = new MQTTTopicsManager(vertx, tenant);
//        store = new MQTTStoreManager(vertx, tenant);

//        boolean clientIDExists = clientIDExists(clientID);
//        if(clientIDExists) {
//            // Resume old session
//            Container.logger().info("Connect ClientID ==> "+ clientID +" alredy exists !!");
//        } else {
//            addClientID(clientID);
//        }
        if(session == null) {
            // alloca comunque la sessione anche se l'id è riutilizzato
            session = new MQTTSession(vertx, this);
            session.handleConnectMessage(connectMessage);
        } else {
            System.out.println("Session alredy allocated with clientID: "+ session.getClientID() +".");
        }

//        if(connectMessage.isWillFlag()) {
//            String willMsg = connectMessage.getWillMessage();
//            byte willQos = connectMessage.getWillQos();
//            String willTopic = connectMessage.getWillTopic();
//            session.storeWillMessage(willMsg, willQos, willTopic);
//        }
    }

    private void handleDisconnect(DisconnectMessage disconnectMessage) {
//        removeClientID(clientID);
//        session.shutdown();
        session.handleDisconnect(disconnectMessage);
        session = null;
    }


//    private void addClientID(String clientID) {
//        vertx.sharedData().getLocalMap("clientIDs").put(clientID, 1);
//    }
//    private boolean clientIDExists(String clientID) {
//        LocalMap<String, Object> m = vertx.sharedData().getLocalMap("clientIDs");
//        if(m!=null)
//            return m.keySet().contains(clientID);
//        return false;
//    }
//    private void removeClientID(String clientID) {
//        vertx.sharedData().getLocalMap("clientIDs").remove(clientID);
//    }

}
