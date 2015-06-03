package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import org.dna.mqtt.moquette.proto.messages.*;

import java.util.*;

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
                _initTenant(tenant);
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
        this.topicsManager = new MQTTTopicsManagerOptimized(this.vertx, this.tenant);
    }
    private void _handleConnectMessage(ConnectMessage connectMessage) {
        if (!cleanSession) {
            Container.logger().info("cleanSession=false: restore old session state with subscriptions ...");
        }
        messageConsumer = vertx.eventBus().consumer(ADDRESS);
        messageConsumer.handler(this);
    }


    public void handlePublishMessage(PublishMessage publishMessage) {
        try {
            Buffer msg = encoder.enc(publishMessage);
            vertx.eventBus().publish(ADDRESS, msg);
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    public void handleSubscribeMessage(SubscribeMessage subscribeMessage) {
        try {
            List<SubscribeMessage.Couple> subs = subscribeMessage.subscriptions();
            for(SubscribeMessage.Couple s : subs) {
                Subscription sub = new Subscription();
                sub.setQos(s.getQos());
                sub.setTopicFilter(s.getTopicFilter());
                this.subscriptions.put(sub.getTopicFilter(), sub);
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

            boolean publishMessageToThisClient = false;
            int maxQos = -1;

            List<Subscription> subs = getAllMatchingSubscriptions(pm);
            if(subs!=null && subs.size()>0) {
                publishMessageToThisClient = true;
                for (Subscription s : subs) {
                    int itemQos = s.getQos();
                    if (itemQos > maxQos) {
                        maxQos = itemQos;
                    }
                }
            }

            if(publishMessageToThisClient) {
                // "the Server MUST deliver the message to the Client respecting the maximum QoS of all the matching subscriptions"

                /* the qos is the max required ... */
                AbstractMessage.QOSType originalQos = pm.getQos();
                int iSentQos = qosUtils.toInt(originalQos);
                int iOkQos = qosUtils.calculatePublishQos(iSentQos, maxQos);
                AbstractMessage.QOSType qos = qosUtils.toQos(iOkQos);
                pm.setQos(qos);

                /* server must send retain=false flag to subscribers ...*/
                pm.setRetainFlag(false);
                sendPublishMessage(pm);
            }
        } catch (Throwable e) {
            Container.logger().error(e.getMessage(), e);
        }
    }

//    /**
//     * check if topic of published message pass at least one of the subscriptions
//     */
//    private boolean isMessageForThisClient(PublishMessage pm) {
//        String topic = pm.getTopicName();
//        boolean publishMessageToThisClient = false;
//        // check if topic of published message pass at least one of the subscriptions
//        for (Subscription c : subscriptions.values()) {
//            String topicFilter = c.getTopicFilter();
//            publishMessageToThisClient = topicsManager.match(topic, topicFilter);
//            if(publishMessageToThisClient) {
//                break;
//            }
//        }
//        return publishMessageToThisClient;
//    }
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
//    private int getMaximumQosOfAllMatchingSubscriptions(PublishMessage pm) {
//        List<Subscription> subs = getAllMatchingSubscriptions(pm);
//        int qos = -1;
//        for (Subscription s : subs) {
//            int itemqos = s.getQos();
//            if(itemqos > qos) {
//                qos = itemqos;
//            }
//        }
//        return qos;
//    }

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
