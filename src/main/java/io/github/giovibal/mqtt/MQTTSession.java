package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.StoreManager;
import io.github.giovibal.mqtt.persistence.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.impl.FutureFactoryImpl;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.FutureFactory;
import org.dna.mqtt.moquette.proto.messages.*;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BooleanSupplier;

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
    private Map<String, List<Subscription>> matchingSubscriptionsCache;

    public MQTTSession(Vertx vertx, ConfigParser config) {
        this.vertx = vertx;
        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.useOAuth2TokenValidation = config.isSecurityEnabled();
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

    public void handleConnectMessage(ConnectMessage connectMessage,
                                     Handler<Boolean> authHandler)
            throws Exception {

        clientID = connectMessage.getClientID();
        cleanSession = connectMessage.isCleanSession();

        // AUTHENTICATION START
        String username = connectMessage.getUsername();
        String password = connectMessage.getPassword();

        if(useOAuth2TokenValidation) {
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
//        messageConsumer = vertx.eventBus().consumer(ADDRESS + tenant);
//        messageConsumer.handler(this);
    }


    public void handlePublishMessage(PublishMessage publishMessage) {
        try {
            if(publishMessage.isRetainFlag()) {
                storeManager.saveRetainMessage(publishMessage);
            }

            int remLen = publishMessage.getRemainingLength();
            Buffer msg = encoder.enc(publishMessage);
            Container.logger().debug( msg.getBytes().length +" "+ remLen +" fixed header length => "+ (msg.getBytes().length - remLen));

            vertx.eventBus().publish(ADDRESS + tenant, msg);
//            if(tenant!=null && tenant.trim().length()>0)
//                vertx.eventBus().publish(ADDRESS, msg);
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    public void handleSubscribeMessage(SubscribeMessage subscribeMessage) {
        try {
            if(this.messageConsumer==null) {
                messageConsumer = vertx.eventBus().consumer(ADDRESS + tenant);
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

                // check in client wants receive retained message by this topicFilter
                boolean clientRefuseRetainMessages = clientRefuseRetainMessages();
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
    private boolean clientRefuseRetainMessages() {
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
        return clientRefuseRetainMessages;
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

            // server must send retain=false flag to subscribers ...
            publishMessage.setRetainFlag(false);
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
         * TODO: implement this behaviour
         * On receipt of DISCONNECT the Server:
         * - MUST discard any Will Message associated with the current connection without publishing it, as described in Section 3.1.2.5 [MQTT-3.14.4-3].
         * - SHOULD close the Network Connection if the Client has not already done so.
         */
        shutdown();
    }
    public void shutdown() {
        //deallocate this instance ...
        if(messageConsumer!=null && cleanSession) {
            messageConsumer.unregister();
            messageConsumer = null;
        }
        vertx = null;
    }

}
