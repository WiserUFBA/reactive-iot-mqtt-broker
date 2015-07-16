package io.github.giovibal.mqtt;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.*;
import io.vertx.core.net.NetSocket;
import io.vertx.core.streams.Pump;

/**
 * Created by giova_000 on 15/07/2015.
 */
public class EventBusNetBridge {
    private static final String BR_HEADER = "bridged";

    private MessageConsumer<Buffer> consumer;
    private MessageProducer<Buffer> producer;
    private NetSocket netSocket;
    private EventBus eventBus;
    private String eventBusAddress;
    private String tenant;

    public EventBusNetBridge(NetSocket netSocket, EventBus eventBus, String eventBusAddress, String tenant) {
        this.eventBus = eventBus;
        this.netSocket = netSocket;
        this.eventBusAddress = eventBusAddress;
        this.tenant = tenant;
    }

    public void start() {

        DeliveryOptions deliveryOpt = new DeliveryOptions().addHeader(BR_HEADER, BR_HEADER);
        if(tenant!=null) {
            deliveryOpt.addHeader(MQTTSession.TENANT_HEADER, tenant);
        }
        MessageConsumer<Buffer> consumer = eventBus.localConsumer(eventBusAddress);
        MessageProducer<Buffer> producer = eventBus.publisher(eventBusAddress, deliveryOpt);

        // from remote tcp to local bus
        Pump.pump(netSocket, producer).start();

        // from local bus to remote tcp
        NetSocketWrapper netSocketWrapper = new MQTTNetSocketWrapper(netSocket);
        consumer.handler(bufferMessage -> {
            boolean isBridged = bufferMessage.headers() != null && bufferMessage.headers().contains(BR_HEADER);
            if (!isBridged) {
                boolean tenantMatch = tenantMatch(bufferMessage);
                if(tenantMatch) {
                    netSocketWrapper.sendMessageToClient(bufferMessage.body());
                }
            }
        });
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
}
