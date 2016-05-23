package io.github.giovibal.mqtt.bridge;

import io.github.giovibal.mqtt.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.*;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketBase;
import io.vertx.core.net.NetSocket;

import java.util.UUID;

/**
 * Created by giova_000 on 15/07/2015.
 */
public class EventBusWebsocketBridge {
    private static final String BR_HEADER = "bridged";

    private WebSocketBase netSocket;
    private EventBus eventBus;
    private String eventBusAddress;
    private String tenant;
    private DeliveryOptions deliveryOpt;
    private MessageConsumer<Buffer> consumer;
    private MessageProducer<Buffer> producer;
    private MqttPump fromRemoteTcpToLocalBus;
    private WebSocketWrapper netSocketWrapper;
    private String bridgeUUID;

//    public EventBusWebsocketBridge(WebSocket netSocket, EventBus eventBus, String eventBusAddress) {
    public EventBusWebsocketBridge(WebSocketBase netSocket, EventBus eventBus, String eventBusAddress) {
        this.eventBus = eventBus;
        this.netSocket = netSocket;
        this.eventBusAddress = eventBusAddress;

        bridgeUUID = UUID.randomUUID().toString();
        deliveryOpt = new DeliveryOptions().addHeader(BR_HEADER, bridgeUUID);
        if(tenant!=null) {
            deliveryOpt.addHeader(MQTTSession.TENANT_HEADER, tenant);
        }
        consumer = eventBus.consumer(eventBusAddress);
        producer = eventBus.publisher(eventBusAddress, deliveryOpt);
        fromRemoteTcpToLocalBus = new MqttPump(netSocket, producer);
        netSocketWrapper = new MQTTWebSocketWrapper(netSocket);

        fromRemoteTcpToLocalBus.setListener(e -> {
            Container.logger().warn("Corrupted message from bridge: "+ e.getMessage());
            netSocket.close();
        });
    }

    public void start() {
        netSocket.pause();
        consumer.pause();
        // from remote tcp to local bus
        fromRemoteTcpToLocalBus.start();

        // from local bus to remote tcp
        consumer.handler(bufferMessage -> {
//            debug(bufferMessage);
            boolean isBridged = bufferMessage.headers() != null
                    && bufferMessage.headers().contains(BR_HEADER)
                    && bufferMessage.headers().get(BR_HEADER).equals(bridgeUUID)
                    ;
            if (!isBridged) {
                boolean tenantMatch = tenantMatch(bufferMessage);
                if(tenantMatch) {
                    netSocketWrapper.sendMessageToClient(bufferMessage.body());
                }
            }
        });
        consumer.resume();
        netSocket.resume();
    }

    // TODO: this method is equal to MQTTSession.isTenantSession, need refactoring
    private boolean isTenantSession() {
        boolean isTenantSession = tenant!=null && tenant.trim().length()>0;
        return isTenantSession;
    }
    // TODO: this method is equal to MQTTSession.tenantMatch, need refactoring
    private boolean tenantMatch(Message<Buffer> message) {
        boolean isTenantSession = isTenantSession();
        boolean tenantMatch;
        if(isTenantSession) {
            boolean containsTenantHeader = message.headers().contains(MQTTSession.TENANT_HEADER);
            if (containsTenantHeader) {
                String tenantHeaderValue = message.headers().get(MQTTSession.TENANT_HEADER);
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

    public String getBridgeUUID() {
        return bridgeUUID;
    }

    public void stop() {
        // from remote tcp to local bus
        fromRemoteTcpToLocalBus.stop();
        // from local bus to remote tcp
        netSocketWrapper.stop();// stop write to remote tcp socket
        consumer.handler(null);// stop read from bus
    }


//    private void debug(Message<Buffer> bufferMessage) {
//        try {
//            Buffer copy = bufferMessage.body().copy();
//            MQTTDecoder dec = new MQTTDecoder();
//            AbstractMessage am = dec.dec(copy);
//            if(am!=null) {
//                if (am instanceof PublishMessage) {
//                    PublishMessage pm = (PublishMessage) am;
//                    String s = pm.getPayloadAsString();
//                    Container.logger().info(s);
//                } else {
//                    Container.logger().error(am.getClass().getSimpleName() + " " + am.isDupFlag());
//                }
//            } else {
//                Container.logger().error("Cannot decode message");
//            }
//        } catch(Throwable e) {
//            Container.logger().error(e.getMessage(), e);
//        }
//    }


    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getTenant() {
        return tenant;
    }
}
