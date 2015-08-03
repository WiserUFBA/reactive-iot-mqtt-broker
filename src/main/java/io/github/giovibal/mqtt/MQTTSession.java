package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.StoreManager;
import io.github.giovibal.mqtt.persistence.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import org.dna.mqtt.moquette.proto.messages.*;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by giovanni on 07/05/2014.
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTSession implements Handler<Message<Buffer>> {

    public static final String ADDRESS = "io.github.giovibal.mqtt";
    public static final String TENANT_HEADER = "tenant";

    private Vertx vertx;
    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    private ITopicsManager topicsManager;
    private String clientID;
    private String protoName;
    private boolean cleanSession;
    private String tenant;
    private boolean useOAuth2TokenValidation;
    private boolean retainSupport;
    private MessageConsumer<Buffer> messageConsumer;
    private Handler<PublishMessage> publishMessageHandler;
    private Map<String, Subscription> subscriptions;
    private QOSUtils qosUtils;
    private StoreManager storeManager;
    private Map<String, List<Subscription>> matchingSubscriptionsCache;
    private PublishMessage willMessage;

//    private int keepAliveSeconds;
    private long keepAliveTimerID;
    private boolean keepAliveTimeEnded;
    private Handler<String> keepaliveErrorHandler;

    public MQTTSession(Vertx vertx, ConfigParser config) {
        this.vertx = vertx;
        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.useOAuth2TokenValidation = config.isSecurityEnabled();
        this.retainSupport = config.isRetainSupport();
        this.subscriptions = new LinkedHashMap<>();
        this.qosUtils = new QOSUtils();
        this.matchingSubscriptionsCache = new HashMap<>();
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

    public void setKeepaliveErrorHandler(Handler<String> keepaliveErrorHandler) {
        this.keepaliveErrorHandler = keepaliveErrorHandler;
    }

    public PublishMessage getWillMessage() {
        return willMessage;
    }

    public void handleConnectMessage(ConnectMessage connectMessage,
                                     Handler<Boolean> authHandler)
            throws Exception {

        clientID = connectMessage.getClientID();
        cleanSession = connectMessage.isCleanSession();
        protoName = connectMessage.getProtocolName();
        if("MQIsdp".equals(protoName)) {
            Container.logger().debug("Detected MQTT v. 3.1 " + protoName + ", clientID: " + clientID);
        } else if("MQTT".equals(protoName)) {
            Container.logger().debug("Detected MQTT v. 3.1.1 " + protoName + ", clientID: " + clientID);
        } else {
            Container.logger().debug("Detected MQTT protocol " + protoName + ", clientID: " + clientID);
        }

        String username = connectMessage.getUsername();
        String password = connectMessage.getPassword();

        if(useOAuth2TokenValidation) {
            // AUTHENTICATION START
            String authorizationAddress = "io.github.giovibal.mqtt.AuthorizationVerticle";
            JsonObject oauth2_token_info = new JsonObject()
                    .put("access_token", username)
                    .put("refresh_token", password);
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
                        Container.logger().info("authorized_user ===> " + authorized_user + ", tenant ===> " + tenant);
                        _handleConnectMessage(connectMessage);
                        authHandler.handle(Boolean.TRUE);
                    } else {
                        Container.logger().info("authenticated error ===> " + error_msg);
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
            Container.logger().debug("cleanSession=false: restore old session state with subscriptions ...");
        }
        boolean isWillFlag = connectMessage.isWillFlag();
        if(isWillFlag) {
            String willMessageM = connectMessage.getWillMessage();
            String willTopic = connectMessage.getWillTopic();
            byte willQosByte = connectMessage.getWillQos();
            AbstractMessage.QOSType willQos = qosUtils.toQos(willQosByte);

            try {
                willMessage = new PublishMessage();
                willMessage.setPayload(willMessageM);
                willMessage.setTopicName(willTopic);
                willMessage.setQos(willQos);
                switch (willQos) {
                    case EXACTLY_ONCE:
                    case LEAST_ONE:
                        willMessage.setMessageID(1);
                }
            } catch (UnsupportedEncodingException e) {
                Container.logger().error(e.getMessage(), e);
            }
        }

//        setKeepAliveSeconds(connectMessage.getKeepAlive());
        startKeepAliveTimer(connectMessage.getKeepAlive());
    }

//    public void setKeepAliveSeconds(int keepAliveSeconds) {
//        this.keepAliveSeconds = keepAliveSeconds;
//    }

    private void startKeepAliveTimer(int keepAliveSeconds) {
        if(keepAliveSeconds > 0) {
            stopKeepAliveTimer();
            keepAliveTimeEnded = true;
            /*
             * If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client
             * within one and a half times the Keep Alive time period, it MUST disconnect
             */
            long keepAliveMillis = keepAliveSeconds * 1500;
            keepAliveTimerID = vertx.setPeriodic(keepAliveMillis, tid -> {
                if(keepAliveTimeEnded) {
                    Container.logger().info("keep alive timer end");
                    handleWillMessage();
                    if (keepaliveErrorHandler != null) {
                        keepaliveErrorHandler.handle(clientID);
                    }
                    stopKeepAliveTimer();
                }
                // next time, will close connection
                keepAliveTimeEnded = true;
            });
        }
    }
    private void stopKeepAliveTimer() {
        try {
            Container.logger().info("keep alive cancel old timer: " + keepAliveTimerID);
            boolean removed = vertx.cancelTimer(keepAliveTimerID);
            if (!removed) {
                Container.logger().info("keep alive cancel old timer not removed: " + keepAliveTimerID);
            }
        } catch(Throwable e) {
            Container.logger().error("Cannot stop KeepAlive Timer with ID: "+keepAliveTimerID, e);
        }
    }

    public void resetKeepAliveTimer() {
        keepAliveTimeEnded = false;
    }


    public void handlePublishMessage(PublishMessage publishMessage) {
        try {
            // publish always have tenant, if session is not tenantized, tenant is retrieved from topic ([tenant]/to/pi/c)
            String publishTenant = calculatePublishTenant(publishMessage);

            // store retained messages ...
            if(publishMessage.isRetainFlag()) {
                boolean payloadIsEmpty=false;
                ByteBuffer bb = publishMessage.getPayload();
                if(bb!=null) {
                    byte[] bytes = bb.array();
                    if (bytes.length == 0) {
                        payloadIsEmpty = true;
                    }
                }
                if(payloadIsEmpty) {
                    storeManager.deleteRetainMessage(publishTenant, publishMessage.getTopicName());
                } else {
                    storeManager.saveRetainMessage(publishTenant, publishMessage);
                }
            }

            /* It MUST set the RETAIN flag to 0 when a PUBLISH Packet is sent to a Client
             * because it matches an established subscription
             * regardless of how the flag was set in the message it received. */
            publishMessage.setRetainFlag(false);
            Buffer msg = encoder.enc(publishMessage);
            if(tenant == null)
                tenant = "";
            DeliveryOptions opt = new DeliveryOptions().addHeader(TENANT_HEADER, publishTenant);
            vertx.eventBus().publish(ADDRESS, msg, opt);
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    private String calculatePublishTenant(PublishMessage publishMessage) {
        return calculatePublishTenant(publishMessage.getTopicName());
    }
    private String calculatePublishTenant(String topic) {
        boolean isTenantSession = isTenantSession();
        if(isTenantSession) {
            return tenant;
        } else {
            String t;
            boolean slashFirst = topic.startsWith("/");
            if (slashFirst) {
                int idx = topic.indexOf('/', 1);
                if(idx>1)
                    t = topic.substring(1, idx);
                else
                    t = topic.substring(1);
            } else {
                int idx = topic.indexOf('/', 0);
                if(idx>0)
                    t = topic.substring(0, idx);
                else
                    t = topic;
            }
            return t;
        }
    }
//    public static void main(String[] args) {
//        String[] topics = {
//                "/tenant.it/prova/topic",
//                "tenant.it/prova/topic",
//                "tenant.it",
//                "/tenant.it",
//                "/tenant.it/tenant.it",
//                ""
//        };
//        for(String topic : topics) {
//            String t;
//            boolean slashFirst = topic.startsWith("/");
//            if (slashFirst) {
//                int idx = topic.indexOf('/', 1);
//                if(idx>1)
//                    t = topic.substring(1, idx);
//                else
//                    t = topic.substring(1);
//            } else {
//                int idx = topic.indexOf('/', 0);
//                if(idx>0)
//                    t = topic.substring(0, idx);
//                else
//                    t = topic;
//            }
//            System.out.println(t);
//        }
//    }

    public void handleSubscribeMessage(SubscribeMessage subscribeMessage) {
        try {
            final int messageID = subscribeMessage.getMessageID();
            if(this.messageConsumer==null) {
                messageConsumer = vertx.eventBus().consumer(ADDRESS);
                messageConsumer.handler(this);
            }

            // invalidate matching topic cache
            matchingSubscriptionsCache.clear();

            List<SubscribeMessage.Couple> subs = subscribeMessage.subscriptions();
            for(SubscribeMessage.Couple s : subs) {
                String topicFilter = s.getTopicFilter();
                Subscription sub = new Subscription();
                sub.setQos(s.getQos());
                sub.setTopicFilter(topicFilter);
                this.subscriptions.put(topicFilter, sub);

                String publishTenant = calculatePublishTenant(topicFilter);

                // check in client wants receive retained message by this topicFilter
                if(retainSupport) {
                    storeManager.getRetainedMessagesByTopicFilter(publishTenant, topicFilter, (List<PublishMessage> retainedMessages) -> {
                        if (retainedMessages != null) {
                            int incrMessageID = messageID;
                            for (PublishMessage retainedMessage : retainedMessages) {
                                switch (retainedMessage.getQos()) {
                                    case LEAST_ONE:
                                    case EXACTLY_ONCE:
                                        retainedMessage.setMessageID(++incrMessageID);
                                }
                                retainedMessage.setRetainFlag(true);
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

    private boolean isTenantSession() {
        boolean isTenantSession = tenant!=null && tenant.trim().length()>0;
        return isTenantSession;
    }

    private boolean tenantMatch(Message<Buffer> message) {
        boolean isTenantSession = isTenantSession();
        boolean tenantMatch;
        if(isTenantSession) {
            boolean containsTenantHeader = message.headers().contains(TENANT_HEADER);
            if (containsTenantHeader) {
                String tenantHeaderValue = message.headers().get(TENANT_HEADER);
                tenantMatch =
                        tenant.equals(tenantHeaderValue)
                                || "".equals(tenantHeaderValue)
                ;
            } else {
                // if message doesn't contains header is not for a tenant-session
                tenantMatch = false;
            }
        } else {
            // if this is not a tenant-session, receive all messages from all tenants
            tenantMatch = true;
        }
        return tenantMatch;
    }

    @Override
    public void handle(Message<Buffer> message) {
        try {
//            boolean isTenantSession = isTenantSession();
//            boolean tenantMatch;
//            if(isTenantSession) {
//                boolean containsTenantHeader = message.headers().contains(TENANT_HEADER);
//                if (containsTenantHeader) {
//                    String tenantHeaderValue = message.headers().get(TENANT_HEADER);
//                    tenantMatch =
//                            tenant.equals(tenantHeaderValue)
//                            || "".equals(tenantHeaderValue)
//                    ;
//                } else {
//                    // if message doesn't contains header is not for a tenant-session
//                    tenantMatch = false;
//                }
//            } else {
//                // if this is not a tenant-session, receive all messages from all tenants
//                tenantMatch = true;
//            }
            boolean tenantMatch = tenantMatch(message);
            if(tenantMatch) {
                Buffer in = message.body();
                PublishMessage pm = (PublishMessage) decoder.dec(in);
                // filter messages by of subscriptions of this client
                handlePublishMessageReceived(pm);
            }
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
                // optimization: if qos==2 is alredy **the max** allowed
                if(maxQos == 2)
                    break;
            }
        }

        if(publishMessageToThisClient) {
            // the qos cannot be bigger than the subscribe requested qos ...
            AbstractMessage.QOSType originalQos = publishMessage.getQos();
            int iSentQos = qosUtils.toInt(originalQos);
            int iOkQos = qosUtils.calculatePublishQos(iSentQos, maxQos);
            AbstractMessage.QOSType qos = qosUtils.toQos(iOkQos);
            publishMessage.setQos(qos);
            sendPublishMessage(publishMessage);
        }
    }

    private List<Subscription> getAllMatchingSubscriptions(PublishMessage pm) {
        List<Subscription> ret = new ArrayList<>();
        String topic = pm.getTopicName();
        if(matchingSubscriptionsCache.containsKey(topic)) {
            return matchingSubscriptionsCache.get(topic);
        }
        // check if topic of published message pass at least one of the subscriptions
        for (Subscription c : subscriptions.values()) {
            String topicFilter = c.getTopicFilter();
            boolean match = topicsManager.match(topic, topicFilter);
            if (match) {
                ret.add(c);
            }
        }
        matchingSubscriptionsCache.put(topic, ret);
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
                    matchingSubscriptionsCache.clear();
                }
            }
        }
        catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    public void handleDisconnect(DisconnectMessage disconnectMessage) {
        Container.logger().debug("Disconnect from " + clientID +" ...");
        /*
         * TODO: implement this behaviour
         * On receipt of DISCONNECT the Server:
         * - MUST discard any Will Message associated with the current connection without publishing it, as described in Section 3.1.2.5 [MQTT-3.14.4-3].
         * - SHOULD close the Network Connection if the Client has not already done so.
         */
        shutdown();
    }
    public void shutdown() {
        // deallocate this instance ...
        if(messageConsumer!=null && cleanSession) {
            messageConsumer.unregister();
            messageConsumer = null;
        }
        // stop keepalive timer
        stopKeepAliveTimer();
        vertx = null;
    }

    public void handleWillMessage() {
        // publish will message if present ...
        if(willMessage != null) {
//            Container.logger().debug("publish will message ... topic[" + willMessage.getTopicName()+"]");
            handlePublishMessage(willMessage);
        }
    }

    public String getClientInfo() {
        String clientInfo ="clientID: "+ clientID +", MQTT protocol: "+ protoName +"";
        return clientInfo;
    }
}
