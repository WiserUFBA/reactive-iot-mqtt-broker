package it.filippetti.smartplatform.mqtt;

import it.filippetti.smartplatform.mqtt.parser.MQTTDecoder;
import it.filippetti.smartplatform.mqtt.parser.MQTTEncoder;
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
                            handlePublishMessage(publish);
                            break;
                        case LEAST_ONE:
                            // 1. Store message
                            // 2. publish message to subscribers
                            // 3. Delete message
                            // 4. <- PubAck
                            storeMessage(publish);
                            handlePublishMessage(publish);
                            deleteMessage(publish);
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
                            storeMessage(publish);
                            handlePublishMessage(publish);
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

                    //deleteMessage(publish); ----> dove trovo il messaggio "publish"?
                    PubCompMessage pubComp = new PubCompMessage();
                    pubComp.setMessageID(pubRel.getMessageID());
                    sendMessageToClient(pubComp);
                    break;
                case DISCONNECT:
                    // TODO:
                    // 1. terminate the session
                    // se il flag "clean_session" del CONNECT era == 0, allora non pulisce le subscriptions di questo client
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



    protected void sendMessageToClient(AbstractMessage message) {
        try {
            Buffer b1 = encoder.enc(message);
            sendMessageToClient(b1);
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }

    protected void handleConnectMessage(ConnectMessage connectMessage) {
        ConnectMessage connect = connectMessage;
        String clientID = connect.getClientID();
        if(connect.isCleanSession()) {
            // 1 sessione per SOLO la durata della connection
        }
        else {
            // la sessione è persistente
            // TODO:
            // 1. prendere dalla vecchia connessione di questo client, tutte le vecchie subscriptions
            // 2. risottoscrivere il client a tutti i topics
        }

        if(connect.isWillFlag()) {
            String willMsg = connect.getWillMessage();
            byte willQos = connect.getWillQos();
            String willTopic = connect.getWillTopic();
            // TODO: fare lo storage di questo willMessage, così da inviarlo al disconnect di questo client
            storeWillMessage(willMsg, willQos, willTopic);
        }
    }


    protected void handlePublishMessage(PublishMessage publishMessage) {
        try {
            String topic = publishMessage.getTopicName();
            JsonObject msg = mqttJson.serializePublishMessage(publishMessage);
//            ByteBuf bb = new Buffer().getByteBuf();
//            encoder.encode(publishMessage, bb);
//            Buffer msgBin = new Buffer(bb);

//            Set<String> topicsSubscribed = vertx.sharedData().getSet("mqtt_topics");
            Set<String> topicsToPublish = topicsManager.calculateTopicsToPublish(topic);

            for (String tpub : topicsToPublish) {
                vertx.eventBus().publish(tpub, msg);
//                vertx.eventBus().publish(tpub, msgBin);
            }
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
                final int iMaxQos = qosUtils.toInt(requestedQos);

                String topic = c.getTopic();
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

//                            Buffer msgBin = (Buffer)message.body();
//                            List<Object> out = new ArrayList<>();
//                            decoder.decode(msgBin.getByteBuf(), out);
//                            PublishMessage pm = (PublishMessage)out.iterator().next();
//                            int iSentQos = qosUtils.toInt(pm.getQos());
//                            int iOkQos = qosUtils.calculatePublishQos(iSentQos, iMaxQos);
//                            pm.setQos(qosUtils.toQos(iOkQos));
//                            pm.setRetainFlag(false);
//                            sendMessageToClient(pm);

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
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }
    protected void handleUnsubscribeMessage(UnsubscribeMessage unsubscribeMessage) {
        try {
            List<String> topics = unsubscribeMessage.topics();
            for (String topic : topics) {
                Set<Handler<Message>> clientHandlers = getClientHandlers(topic);
                for (Handler<Message> handler : clientHandlers) {
                    vertx.eventBus().unregisterHandler(topic, handler);
                    topicsManager.removeSubscribedTopic(topic);
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
    abstract protected void storeMessage(PublishMessage publishMessage);
    abstract protected void deleteMessage(PublishMessage publishMessage);
    abstract protected void storeWillMessage(String willMsg, byte willQos, String willTopic);

}
