package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.MQTTStoreManager;
import io.github.giovibal.mqtt.persistence.MQTTStoreManagerAsync;
import io.github.giovibal.mqtt.persistence.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.*;
import io.vertx.core.json.JsonObject;
import org.dna.mqtt.moquette.proto.messages.*;

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
//    private MQTTJson mqttJson;
    private QOSUtils qosUtils;
    private Map<String, Set<MessageConsumer>> handlers;
    private ITopicsManager topicsManager;
    private String clientID;
    private boolean cleanSession;
//    private MQTTStoreManager store;
    private MQTTStoreManagerAsync store;
    private String tenant;

    private boolean useOAuth2TokenValidation;

    public MQTTSession(Vertx vertx, ConfigParser config) {
        this.vertx = vertx;
        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.qosUtils = new QOSUtils();
        this.handlers = new HashMap<>();
        this.useOAuth2TokenValidation = config.isSecurityEnabled();
    }

    public String getClientID() {
        return clientID;
    }

    private String extractTenant(String username) {
        if(username == null || username.trim().length()==0)
            return "";
        String tenant = "";
        int idx = username.lastIndexOf('@');
        if(idx > 0) {
            tenant = username.substring(idx+1);
        }
        return tenant;
    }

    public void handleConnectMessage(ConnectMessage connectMessage,
                                     final Handler<PublishMessage> handler,
                                     Handler<Boolean> authHandler)
            throws Exception {
        clientID = connectMessage.getClientID();
        cleanSession = connectMessage.isCleanSession();

        // AUTHENTICATION START
        String username = connectMessage.getUsername();
        String password = connectMessage.getPassword();

        if(useOAuth2TokenValidation) {
            String authorizationAddress = AuthorizationVerticle.class.getName();
            JsonObject oauth2_token_info = new JsonObject().put("access_token", username).put("refresh_token", password);
            vertx.eventBus().send(authorizationAddress, oauth2_token_info, (AsyncResult<Message<JsonObject>> res) -> {
                if (res.succeeded()) {
                    JsonObject validationInfo = res.result().body();
                    Container.logger().info(validationInfo);
                    Boolean token_valid = validationInfo.getBoolean("token_valid", Boolean.FALSE);
                    String authorized_user = validationInfo.getString("authorized_user");
                    String error_msg = validationInfo.getString("error_msg");
                    Container.logger().info("authenticated ===> " + token_valid);
                    if (token_valid) {
                        tenant = extractTenant(authorized_user);
                        Container.logger().info("authorized_user ===> " + authorized_user +", tenant ===> "+ tenant);
                        _handleConnectMessage(connectMessage, handler);
                        authHandler.handle(Boolean.TRUE);
                    } else {
                        Container.logger().info("authenticated error ===> "+ error_msg);
                        authHandler.handle(Boolean.FALSE);
                    }
                } else {
                    Container.logger().info("login failed !");
                    authHandler.handle(Boolean.FALSE);
                }
            });
        }
        else {
            String clientID = connectMessage.getClientID();
            if(username == null || username.trim().length()==0)
                tenant = extractTenant(clientID);
            else
                tenant = extractTenant(username);
            _handleConnectMessage(connectMessage, handler);
            authHandler.handle(Boolean.TRUE);
        }
    }
    private void _handleConnectMessage(ConnectMessage connectMessage, final Handler<PublishMessage> handler) {
//        topicsManager = new MQTTTopicsManager(this.vertx, this.tenant);
        topicsManager = new MQTTTopicsManagerOptimized(this.vertx, this.tenant);
//        store = new MQTTStoreManager(this.vertx, this.tenant);
        store = new MQTTStoreManagerAsync(this.vertx, this.tenant);

        // save clientID
        store.clientIDExists(this.clientID, clientIDExists -> {
            if (clientIDExists) {
                // Resume old session
                Container.logger().info("Connect ClientID ==> " + clientID + " alredy exists !!");
            } else {
                store.addClientID(clientID);
            }

            if (connectMessage.isWillFlag()) {
                String willMsg = connectMessage.getWillMessage();
                byte willQos = connectMessage.getWillQos();
                String willTopic = connectMessage.getWillTopic();
                storeWillMessage(willMsg, willQos, willTopic);
            }

            if (!cleanSession) {
                Container.logger().info("cleanSession=false: resubscribe previous topics ...");
                store.getSubscriptionsByClientID(clientID, subs -> {
                    if (subs != null) {
                        for (Subscription s : subs) {
                            subscribeClientToTopic(s.getTopic(), s.getQos(), handler);
                            Container.logger().info("cleanSession=false: resubscribed " + s);
                        }
                    }
                });
            }
        });
    }

    public void handleDisconnect(DisconnectMessage disconnectMessage) {
        shutdown();
    }

    public void handlePublishMessage(PublishMessage publishMessage) {
        try {
            String topic = publishMessage.getTopicName();
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


    public void handleSubscribeMessage(SubscribeMessage subscribeMessage, Handler<PublishMessage> handler) {
        try {
            List<SubscribeMessage.Couple> subs = subscribeMessage.subscriptions();
            for (SubscribeMessage.Couple c : subs) {
                byte requestedQosByte = c.getQos();
                final QOSType requestedQos = qosUtils.toQos(requestedQosByte);
                final int iRequestedQos = qosUtils.toInt(requestedQos);
                String topic = c.getTopicFilter();
                subscribeClientToTopic(topic, iRequestedQos, handler);
                if(clientID!=null && !cleanSession) {
                    Subscription s = new Subscription();
                    s.setQos(requestedQosByte);
                    s.setTopic(topic);
                    store.saveSubscription(s, clientID);
                }
                // replay saved messages
                republishPendingMessagesForSubscription(topic, handler);
            }
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    private void republishPendingMessagesForSubscription(String topic, final Handler<PublishMessage> handler) {
        // re-publish
        store.getMessagesByTopic(topic, clientID, (List<byte[]> messages) -> {
            for(byte[] message : messages) {
                try {
                    // publish message to this client
                    PublishMessage pm = (PublishMessage) decoder.dec(Buffer.buffer(message));
                    // send message directly to THIS client
                    // check if message topic matches topicFilter of subscription
                    boolean ok = topicsManager.match(pm.getTopicName(), topic);
                    if (ok) {
//                        mqttSocket.sendMessageToClient(pm);
                        handler.handle(pm);
                    }
                    // delete will happen when publish end correctly.
                    deleteMessage(pm);
                }
                catch(Exception e) {
                    Container.logger().error(e.getMessage(), e);
                }
            }
        });
    }

    private void subscribeClientToTopic(final String topic, int requestedQos, final Handler<PublishMessage> subHandler) {
        final int iMaxQos = requestedQos;
        Handler<Message<Buffer>> handler = message -> {
            try {
                System.out.printf("Message arrived from address: %s\n", message.address());
                Buffer in = message.body();
                PublishMessage pm = (PublishMessage)decoder.dec(in);
                /* the qos is the max required ... */
                QOSType originalQos = pm.getQos();
                int iSentQos = qosUtils.toInt(originalQos);
                int iOkQos = qosUtils.calculatePublishQos(iSentQos, iMaxQos);
                pm.setQos(qosUtils.toQos(iOkQos));
                /* server must send retain=false flag to subscribers ...*/
                pm.setRetainFlag(false);
//                mqttSocket.sendMessageToClient(pm);
                subHandler.handle(pm);
            } catch (Throwable e) {
                Container.logger().error(e.getMessage(), e);
            }
        };
        MessageConsumer<Buffer> consumer = vertx.eventBus().consumer(toVertxTopic(topic));
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
                store.popMessage(tsub, clientID, popped -> {});
            }
        } catch(Exception e) {
            Container.logger().error(e.getMessage(), e);
        }
    }


    public void storeWillMessage(String willMsg, byte willQos, String willTopic) {
        store.storeWillMessage(willMsg, willQos, willTopic);
    }

    public void shutdown() {
        //deallocate this instance ...
        Set<String> topics = handlers.keySet();
        for (String topic : topics) {
            Set<MessageConsumer> clientHandlers = getClientHandlers(topic);
            for (MessageConsumer messageConsumer : clientHandlers) {
                messageConsumer.unregister();
                topicsManager.removeSubscribedTopic(topic);
                if (clientID != null && cleanSession) {
                    if(store!=null) {
                        store.deleteSubcription(topic, clientID);
                    }
                }
            }
        }
        if(store!=null) {
            store.removeClientID(clientID);
        }
//        mqttSocket = null;
        vertx = null;
    }
}
