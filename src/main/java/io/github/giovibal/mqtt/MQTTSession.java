package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.StoreManager;
import io.github.giovibal.mqtt.persistence.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import org.dna.mqtt.moquette.proto.messages.*;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by giovanni on 07/05/2014.
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTSession implements Handler<Message<Buffer>> {

    public static final String ADDRESS = "io.github.giovibal.mqtt";

    private Vertx vertx;
    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    private ITopicsManager topicsManager;
    private String clientID;
    private boolean cleanSession;
    private String tenant;
    private boolean useOAuth2TokenValidation;

    private MessageConsumer<Buffer> messageConsumer;
    private Handler<PublishMessage> publishMessageHandler;
    private Map<String, Subscription> subscriptions;

    private QOSUtils qosUtils;
    private StoreManager storeManager;

    private PublishMessage willMessage;

    public MQTTSession(Vertx vertx, ConfigParser config) {
        this.vertx = vertx;
        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.useOAuth2TokenValidation = config.isSecurityEnabled();
        this.subscriptions = new LinkedHashMap<>();
        this.qosUtils = new QOSUtils();
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

    public void setPublishMessageHandler(Handler<PublishMessage> publishMessageHandler) {
        this.publishMessageHandler = publishMessageHandler;
    }

    public void handleConnectMessage(ConnectMessage connectMessage,
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
                        String tenant = extractTenant(authorized_user);
                        _initTenant(tenant);
                        Container.logger().info("authorized_user ===> " + authorized_user +", tenant ===> "+ tenant);
                        _handleConnectMessage(connectMessage);
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
            String tenant = null;
            if(username == null || username.trim().length()==0) {
                tenant = extractTenant(clientID);
            }
            else {
                tenant = extractTenant(username);
            }
            _initTenant(tenant);
            _handleConnectMessage(connectMessage);
            authHandler.handle(Boolean.TRUE);
        }
    }
    private void _initTenant(String tenant) {
        if(tenant == null)
            throw new IllegalStateException("Tenant cannot be null");
        this.tenant = tenant;
        this.topicsManager = new MQTTTopicsManagerOptimized();
        this.storeManager = new StoreManager(this.vertx, this.tenant, this.topicsManager);
    }
    private void _handleConnectMessage(ConnectMessage connectMessage) {
        if (!cleanSession) {
            Container.logger().info("cleanSession=false: restore old session state with subscriptions ...");
        }
        boolean isWillFlag = connectMessage.isWillFlag();
        if(isWillFlag) {
            String willMessageM = connectMessage.getWillMessage();
            String willTopic = connectMessage.getWillTopic();
            byte willQosByte = connectMessage.getWillQos();
            AbstractMessage.QOSType willQos = qosUtils.toQos(willQosByte);

//            JsonObject will = new JsonObject()
//                    .put("topicName", willTopic)
//                    .put("qos", willQos.ordinal())
//                    .put("message", willMessage);

            try {
                willMessage = new PublishMessage();
                willMessage.setPayload(willMessageM);
                willMessage.setTopicName(willTopic);
                willMessage.setQos(willQos);
            } catch (UnsupportedEncodingException e) {
                Container.logger().error(e.getMessage(), e);
            }

        }
        messageConsumer = vertx.eventBus().consumer(ADDRESS + tenant);
        messageConsumer.handler(this);
    }


    public void handlePublishMessage(PublishMessage publishMessage) {
        try {
            if(publishMessage.isRetainFlag()) {
                storeManager.saveRetainMessage(publishMessage, onComplete -> {});
            }

            Buffer msg = encoder.enc(publishMessage);
            vertx.eventBus().publish(ADDRESS + tenant, msg);
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    public void handleSubscribeMessage(SubscribeMessage subscribeMessage) {
        try {
            List<SubscribeMessage.Couple> subs = subscribeMessage.subscriptions();
            for(SubscribeMessage.Couple s : subs) {
                String topicFilter = s.getTopicFilter();
                Subscription sub = new Subscription();
                sub.setQos(s.getQos());
                sub.setTopicFilter(topicFilter);
                this.subscriptions.put(sub.getTopicFilter(), sub);

                // receive retained message by this topicFilter
                boolean clientRefuseRetainMessages = false;
                if(willMessage!=null && willMessage.getTopicName().equals("$SYS/config")) {
                    try {
                        String willConfig = new String( willMessage.getPayload().array(), "UTF-8" );
                        JsonObject configJson = new JsonObject(willConfig);
                        if(configJson.containsKey("retain")) {
                            Boolean retainSupport = configJson.getBoolean("retain");
                            clientRefuseRetainMessages = retainSupport != null && !retainSupport;
                        }
                    } catch(Throwable e) {
                        Container.logger().warn(e.getMessage(), e);
                    }
                }
                if(!clientRefuseRetainMessages) {
                    storeManager.getRetainedMessagesByTopicFilter(topicFilter, (List<PublishMessage> retainedMessages) -> {
                        if (retainedMessages != null) {
                            for (PublishMessage retainedMessage : retainedMessages) {
                                handlePublishMessageReceived(retainedMessage);
                            }
                        }
                    });
                }
            }
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    @Override
    public void handle(Message<Buffer> message) {
        //filter messages in base of subscriptions of this client
        try {
            Buffer in = message.body();
            PublishMessage pm = (PublishMessage) decoder.dec(in);
            handlePublishMessageReceived(pm);
        } catch (Throwable e) {
            Container.logger().error(e.getMessage(), e);
        }
    }

    public void handlePublishMessageReceived(PublishMessage publishMessage) {
        boolean publishMessageToThisClient = false;
        int maxQos = -1;

        /*
         * the Server MUST deliver the message to the Client respecting the maximum QoS of all the matching subscriptions
         */
        List<Subscription> subs = getAllMatchingSubscriptions(publishMessage);
        if(subs!=null && subs.size()>0) {
            publishMessageToThisClient = true;
            for (Subscription s : subs) {
                int itemQos = s.getQos();
                if (itemQos > maxQos) {
                    maxQos = itemQos;
                }
            }
        }

        /*
         * When sending a PUBLISH Packet to a Client the Server MUST set the RETAIN flag to 1
         * if a message is sent as a result of a new subscription being made by a Client [MQTT-3.3.1-8].
         *
         * It MUST set the RETAIN flag to 0 when a PUBLISH Packet is sent to a Client because it matches an established subscription
         * regardless of how the flag was set in the message it received [MQTT-3.3.1-9].
         */
        publishMessage.setRetainFlag(false);

        if(publishMessageToThisClient) {

            /* the qos is the max required ... */
            AbstractMessage.QOSType originalQos = publishMessage.getQos();
            int iSentQos = qosUtils.toInt(originalQos);
            int iOkQos = qosUtils.calculatePublishQos(iSentQos, maxQos);
            AbstractMessage.QOSType qos = qosUtils.toQos(iOkQos);
            publishMessage.setQos(qos);

            /* server must send retain=false flag to subscribers ...*/
            publishMessage.setRetainFlag(false);
            sendPublishMessage(publishMessage);
        }
    }

    private List<Subscription> getAllMatchingSubscriptions(PublishMessage pm) {
        List<Subscription> ret = new ArrayList<>();
        String topic = pm.getTopicName();
        // check if topic of published message pass at least one of the subscriptions
        for (Subscription c : subscriptions.values()) {
            String topicFilter = c.getTopicFilter();
            boolean match = topicsManager.match(topic, topicFilter);
            if(match) {
                ret.add(c);
            }
        }
        return ret;
    }

    private void sendPublishMessage(PublishMessage pm) {
        if(publishMessageHandler!=null)
            publishMessageHandler.handle(pm);
    }



    public void handleUnsubscribeMessage(UnsubscribeMessage unsubscribeMessage) {
        try {
            List<String> topicFilterSet = unsubscribeMessage.topicFilters();
            for (String topicFilter : topicFilterSet) {
                if(subscriptions!=null) {
                    subscriptions.remove(topicFilter);
                }
            }
        }
        catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    public void handleDisconnect(DisconnectMessage disconnectMessage) {
        Container.logger().info("Disconnect from "+ clientID +" ...");
        /*
        TODO: implement this behaviour
        On receipt of DISCONNECT the Server:
        - MUST discard any Will Message associated with the current connection without publishing it, as described in Section 3.1.2.5 [MQTT-3.14.4-3].
        - SHOULD close the Network Connection if the Client has not already done so.
         */
        shutdown();
    }
    public void shutdown() {
        //deallocate this instance ...
        if(messageConsumer!=null && cleanSession) {
            messageConsumer.unregister();
        }
        vertx = null;
    }

}
