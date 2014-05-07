package it.filippetti.smartplatform.mqtt;

import io.netty.buffer.ByteBuf;
import it.filippetti.smartplatform.mqtt.parser.MQTTDecoder;
import it.filippetti.smartplatform.mqtt.parser.MQTTEncoder;
import org.dna.mqtt.moquette.proto.Utils;
import org.dna.mqtt.moquette.proto.messages.*;
import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.platform.Verticle;

import java.util.*;

import static org.dna.mqtt.moquette.proto.messages.AbstractMessage.*;

/**
 * Created by giovanni on 11/04/2014.
 * @deprecated use MQTTBroker
 */
public class MQTTBrokerVerticle extends Verticle {

    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    private MQTTJson mqttJson;


//    private static Map<String, Set<Handler<Message>>> handlers = new HashMap<>();
    private Set<Handler<Message>> getClientHandlers(NetSocket netSocket, String topic, Map<String, Set<Handler<Message>>> handlers) {
        String sessionID = netSocket.writeHandlerID()+"_"+topic;
        if(!handlers.containsKey(sessionID)) {
            handlers.put(sessionID, new HashSet<Handler<Message>>());
        }
        Set<Handler<Message>> clientHandlers = handlers.get(sessionID);
        return clientHandlers;
    }
    private void clearClientHandlers(NetSocket netSocket, String topic, Map<String, Set<Handler<Message>>> handlers) {
        String sessionID = netSocket.writeHandlerID()+"_"+topic;
        if(handlers.containsKey(sessionID)) {
            handlers.remove(sessionID);
        }
    }


    @Override
    public void start() {
        try {
            decoder = new MQTTDecoder();
            encoder = new MQTTEncoder();
            mqttJson = new MQTTJson();

            NetServer netServer = vertx.createNetServer();
            netServer.connectHandler(new Handler<NetSocket>() {

                @Override
                public void handle(final NetSocket netSocket) {
                    final Map<String, Set<Handler<Message>>> handlers = new HashMap<>();
                    final MQTTTokenizer tokenizer = new MQTTTokenizer();

//                    final MQTTDecoder decoder = new MQTTDecoder();
//                    final MQTTEncoder encoder = new MQTTEncoder();
//                    final MQTTJson mqttJson = new MQTTJson();

                    tokenizer.registerListener(new MQTTTokenizer.MqttTokenizerListener() {

                        @Override
                        public void onToken(byte[] token, boolean timeout) {

                            Buffer buffer = new Buffer(token);
                            ByteBuf in = buffer.getByteBuf();
                            ArrayList<Object> out = new ArrayList<>();
                            try {
                                decoder.decode(in, out);
                                for(Object message : out) {
                                    onMessageFromClient(netSocket, message, buffer, handlers);
                                }
                            }
                            catch (Exception e) {
                                container.logger().error(e.getMessage(), e);
                            }
                        }
                    });

                    netSocket.dataHandler(new Handler<Buffer>() {

                        @Override
                        public void handle(Buffer buffer) {
//                            ByteBuf in = buffer.getByteBuf();
//                            ArrayList<Object> out = new ArrayList<>();
//                            try {
//                                decoder.decode(in, out);
//                                for(Object message : out) {
//                                    onMessageFromClient(netSocket, message, buffer, handlers);
//                                }
//                            }
//                            catch (Exception e) {
//                                container.logger().error(e.getMessage(), e);
//                            }
                            tokenizer.process(buffer.getBytes());
                        }
                    });
                }
            });

            netServer.listen(1883);
            container.logger().info("Startd MQTT Broker on port: "+ 1883);
        } catch(Exception e ) {
            container.logger().error(e.getMessage(), e);
        }

//        try {
//            vertx.setPeriodic(5000, new Handler<Long>() {
//                @Override
//                public void handle(Long aLong) {
//                    String stats = "Stats: ";
//                    for (String key : handlers.keySet()) {
//                        Set<Handler<Message>> val = handlers.get(key);
//                        for(Handler<Message> h : val) {
//                            stats += "\n"+ key + " => " + h;
//                        }
//                    }
//                    stats += "\nSubscriptions: ";
//                    Set<String> topicsSubscribed = vertx.sharedData().getSet("mqtt_topics");
//                    for(String t : topicsSubscribed) {
//                        stats += "\n"+ t +"";
//                    }
//                    container.logger().info(stats);
//                }
//            });
//        }catch(Throwable e) {
//            container.logger().error(e.getMessage(), e);
//        }
    }

    @Override
    public void stop() {
    }

    private void trace(AbstractMessage msg, String dir) {
        Integer qos = null;
        if(msg.getQos()!=null) {
            qos = new QOSUtils().toInt(msg.getQos());
        }
        String mtype = Utils.msgType2String(msg.getMessageType());
        if(msg instanceof MessageIDMessage) {
            container.logger().info(dir +" "+ mtype +" qos:"+ qos +" msgID:"+ ((MessageIDMessage)msg).getMessageID());
        } else {
            container.logger().info(dir +" "+ mtype +" qos:"+ qos);
        }
    }

    private void onMessageFromClient(NetSocket netSocket, Object message, Buffer buffer, final Map<String, Set<Handler<Message>>> handlers) {
        try {
            AbstractMessage msg = (AbstractMessage) message;
//            trace(msg, " >> ");
            switch (msg.getMessageType()) {
                case CONNECT:
                    ConnectMessage connect = (ConnectMessage)msg;
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
                    }
                    ConnAckMessage connAck = new ConnAckMessage();
                    sendMessageToClient(netSocket, connAck);
                    break;
                case SUBSCRIBE:
                    SubscribeMessage subscribeMessage = (SubscribeMessage)msg;
                    handleSubscribeMessage(netSocket, subscribeMessage, handlers);
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
                    sendMessageToClient(netSocket, subAck);
                    break;
                case UNSUBSCRIBE:
                    UnsubscribeMessage unsubscribeMessage = (UnsubscribeMessage)msg;
                    handleUnsubscribeMessage(netSocket, unsubscribeMessage, handlers);
                    UnsubAckMessage unsubAck = new UnsubAckMessage();
                    unsubAck.setMessageID(unsubscribeMessage.getMessageID());
                    sendMessageToClient(netSocket, unsubAck);
                    break;
                case PUBLISH:
                    PublishMessage publish = (PublishMessage)msg;
                    switch (publish.getQos()) {
                        case RESERVED:
                            break;
                        case MOST_ONE:
                            // 1. publish message to subscribers
                            handlePublishMessage(publish, buffer, handlers);
                            break;
                        case LEAST_ONE:
                            // 1. Store message
                            // 2. publish message to subscribers
                            // 3. Delete message
                            // 4. <- PubAck
                            storeMessage(publish);
                            handlePublishMessage(publish, buffer, handlers);
                            deleteMessage(publish);
                            PubAckMessage pubAck = new PubAckMessage();
                            pubAck.setMessageID(publish.getMessageID());
                            sendMessageToClient(netSocket, pubAck);
                            break;

                        case EXACTLY_ONCE:
                            // 1. Store message
                            // 2. publish message to subscribers
                            // 3. <- PubRec
                            // 4. -> PubRel from client
                            // 5. Delete message
                            // 5. <- PubComp
                            storeMessage(publish);
                            handlePublishMessage(publish, buffer, handlers);
                            PubRecMessage pubRec = new PubRecMessage();
                            pubRec.setMessageID(publish.getMessageID());
                            sendMessageToClient(netSocket, pubRec);
                            break;
                    }
                    break;
                case PUBREC:
                    PubRecMessage pubRec = (PubRecMessage)msg;
                    PubRelMessage prelResp = new PubRelMessage();
                    prelResp.setMessageID(pubRec.getMessageID());
                    sendMessageToClient(netSocket, prelResp);
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
                    sendMessageToClient(netSocket, pubComp);
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
                    sendMessageToClient(netSocket, pingResp);
                    break;
                default:
                    container.logger().warn("type of message not known: "+ msg.getClass().getSimpleName());
                    break;
            }
        } catch (Exception ex) {
            container.logger().error("Bad error in processing the message", ex);
        }
    }

    private void sendMessageToClient(NetSocket netSocket, AbstractMessage message) {
        try {
//            Buffer b1 = new Buffer(2 + message.getRemainingLength());
            Buffer b1 = new Buffer();
            ByteBuf buff1 = b1.getByteBuf();
            encoder.encode(message, buff1);
            b1 = new Buffer(buff1);
//            netSocket.write(b1);
//            trace(message, " << ");
//            container.logger().info(
//                    Thread.currentThread().getId()+" "+Thread.currentThread().getName()
//                            +" writed "+ b1.length() +" bytes to socket.writeHandlerID"
//                            +" => "+ netSocket.writeHandlerID());
            sendMessageToClient(netSocket, b1);

        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }
    private void sendMessageToClient(final NetSocket netSocket, Buffer bytes) {
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
    private void storeMessage(PublishMessage publishMessage) {

    }
    private void deleteMessage(PublishMessage publishMessage) {

    }
    private void handlePublishMessage(PublishMessage publishMessage, Buffer buffer, final Map<String, Set<Handler<Message>>> handlers) {
        try {
            String topic = publishMessage.getTopicName();
//            JsonObject msg = mqttJson.serializePublishMessage(publishMessage);
            ByteBuf bb = new Buffer().getByteBuf();
            encoder.encode(publishMessage, bb);
            Buffer msgBin = new Buffer(bb);

            Set<String> topicsSubscribed = vertx.sharedData().getSet("mqtt_topics");
            Set<String> topicsToPublish = new LinkedHashSet<>();
            for (String tsub : topicsSubscribed) {
                if (tsub.contains("+")) {
                    String pattern = tsub.replaceAll("\\+", ".+?");
                    int topicSlashCount = topic.replaceAll("[^/]", "").length();
                    int tsubSlashCount = tsub.replaceAll("[^/]", "").length();
                    if (topicSlashCount == tsubSlashCount) {
                        if (topic.matches(pattern)) {
                            topicsToPublish.add(tsub);
                        }
                    }
                } else if (tsub.contains("#")) {
                    String pattern = tsub.replaceAll("/#", "/.+");
                    int topicSlashCount = topic.replaceAll("[^/]", "").length();
                    int tsubSlashCount = tsub.replaceAll("[^/]", "").length();
                    if (topicSlashCount >= tsubSlashCount) {
                        if (topic.matches(pattern)) {
                            topicsToPublish.add(tsub);
                        }
                    }
                } else if(topic.equals(tsub)) {
                    topicsToPublish.add(tsub);
                }
            }

            for (String tpub : topicsToPublish) {
//                vertx.eventBus().publish(tpub, msg);
                vertx.eventBus().publish(tpub, msgBin);
//                vertx.eventBus().publish(tpub, buffer);
            }
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }

    private QOSType toQos(byte qosByte) {
        return new QOSUtils().toQos(qosByte);
    }

    private void handleSubscribeMessage(final NetSocket netSocket, SubscribeMessage subscribeMessage, Map<String, Set<Handler<Message>>> handlers) throws Exception {
        try {
            List<SubscribeMessage.Couple> subs = subscribeMessage.subscriptions();
            for (SubscribeMessage.Couple c : subs) {
                byte requestedQosByte = c.getQos();
                final QOSType requestedQos = toQos(requestedQosByte);
                final QOSUtils qosUtils = new QOSUtils();
                final int iMaxQos = qosUtils.toInt(requestedQos);

                String topic = c.getTopic();
                Handler<Message> handler = new Handler<Message>() {
                    @Override
                    public void handle(Message message) {
                        try {
//                            JsonObject json = (JsonObject) message.body();
//                            PublishMessage pm = mqttJson.deserializePublishMessage(json);
//                            // il qos è quello MASSIMO RICHIESTO
//                            int iMaxQos = qosUtils.toInt(qos);
//                            int iSentQos = qosUtils.toInt(pm.getQos());
//                            int iOkQos;
//                            if (iSentQos < iMaxQos)
//                                iOkQos = iSentQos;
//                            else
//                                iOkQos = iMaxQos;
//                            pm.setQos(qosUtils.toQos(iOkQos));
//
//                            pm.setRetainFlag(false);// server must send retain=false flag to subscribers ...
//                            sendMessageToClient(netSocket, pm);


//                            Buffer msgBin = (Buffer)message.body();
//                            sendMessageToClient(netSocket, msgBin);

//                            PublishMessage pm = new PublishMessage();
//                            pm.setMessageID(2);
//                            pm.setTopicName("test/untopic");
//                            QOSType qos = QOSType.values()[iMaxQos];
//                            pm.setQos(qos);
//                            pm.setRetainFlag(false);
//                            ByteBuffer bb = Charset.forName("UTF-8")
//                                    .newEncoder()
//                                    .encode(CharBuffer.wrap(
//                                            "hard coded a dasd asd asd asd asd asd asd asd asd asd asd asd asd asd asd asd asd asd a"
//                                    ));
//                            pm.setPayload(bb);
//                            sendMessageToClient(netSocket, pm);

                            Buffer msgBin = (Buffer)message.body();
                            List<Object> out = new ArrayList<>();
                            decoder.decode(msgBin.getByteBuf(), out);
                            PublishMessage pm = (PublishMessage)out.iterator().next();
                            int iSentQos = qosUtils.toInt(pm.getQos());
                            int iOkQos = qosUtils.calculatePublishQos(iSentQos, iMaxQos);
                            pm.setQos(qosUtils.toQos(iOkQos));
                            pm.setRetainFlag(false);
                            sendMessageToClient(netSocket, pm);

                        } catch (Throwable e) {
                            container.logger().error(e.getMessage(), e);
                        }
                    }
                };
                Set<Handler<Message>> clientHandlers = getClientHandlers(netSocket, topic, handlers);
                clientHandlers.add(handler);
                vertx.eventBus().registerHandler(topic, handler);
                vertx.sharedData().getSet("mqtt_topics").add(topic);
            }
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }
    private void handleUnsubscribeMessage(NetSocket netSocket, UnsubscribeMessage unsubscribeMessage, Map<String, Set<Handler<Message>>> handlers) {
        try {
            List<String> topics = unsubscribeMessage.topics();
            for (String topic : topics) {
                Set<Handler<Message>> clientHandlers = getClientHandlers(netSocket, topic, handlers);
                for (Handler<Message> handler : clientHandlers) {
                    vertx.eventBus().unregisterHandler(topic, handler);
                    vertx.sharedData().getSet("mqtt_topics").remove(topic);
                }
                clearClientHandlers(netSocket, topic, handlers);
            }
        }
        catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }



}
