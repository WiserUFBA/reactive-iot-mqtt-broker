package io.github.giovibal.mqtt.bridge;

import io.github.giovibal.mqtt.Container;
import io.github.giovibal.mqtt.MQTTSession;
import io.github.giovibal.mqtt.security.CertInfo;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;
import io.vertx.core.parsetools.RecordParser;

/**
 * Created by Giovanni Baleani on 15/07/2015.
 */
public class EventBusBridgeServerVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {

        JsonObject conf = config();

        String address = MQTTSession.ADDRESS;
        Integer localBridgePort = conf.getInteger("local_bridge_port", 7007);
        int idelTimeout = conf.getInteger("socket_idle_timeout", 30);


        // [TCP -> BUS] listen TCP publish to BUS
        NetServerOptions opt = new NetServerOptions()
                .setTcpKeepAlive(true)
                .setIdleTimeout(idelTimeout)
                .setPort(localBridgePort)
        ;

        String ssl_cert_key = conf.getString("ssl_cert_key");
        String ssl_cert = conf.getString("ssl_cert");
        String ssl_trust = conf.getString("ssl_trust");
        if(ssl_cert_key != null && ssl_cert != null && ssl_trust != null) {
            opt.setSsl(true).setClientAuth(ClientAuth.REQUIRED)
                .setPemKeyCertOptions(new PemKeyCertOptions()
                    .setKeyPath(ssl_cert_key)
                    .setCertPath(ssl_cert)
                )
                .setPemTrustOptions(new PemTrustOptions()
                    .addCertPath(ssl_trust)
                )
            ;
        }
        NetServer netServer = vertx.createNetServer(opt);
        netServer.connectHandler(netSocket -> {
            final EventBusNetBridge ebnb = new EventBusNetBridge(netSocket, vertx.eventBus(), address);
            netSocket.closeHandler(aVoid -> {
                Container.logger().info("Bridge Server - closed connection from client ip: " + netSocket.remoteAddress());
                ebnb.stop();
            });
            netSocket.exceptionHandler(throwable -> {
                Container.logger().error("Bridge Server - Exception: " + throwable.getMessage(), throwable);
                ebnb.stop();
            });

            Container.logger().info("Bridge Server - new connection from client ip: " + netSocket.remoteAddress());



            final RecordParser parser = RecordParser.newDelimited("\n", h -> {
                String cmd = h.toString();
                if("START SESSION".equalsIgnoreCase(cmd)) {
                    netSocket.pause();
                    ebnb.start();
                    Container.logger().info("Bridge Server - start session with tenant: " + ebnb.getTenant() +", ip: " + netSocket.remoteAddress() +", bridgeUUID: " + ebnb.getBridgeUUID());
                    netSocket.resume();
                } else {
                    String tenant = cmd;
                    String tenantFromCert = new CertInfo(netSocket).getTenant();
//                    if(!tenant.equals(tenantFromCert))
//                        throw new IllegalAccessError("Bridge Authentication Failed for tenant: "+ tenant +"/"+ tenantFromCert);
                    if(tenantFromCert != null)
                        tenant = tenantFromCert;

                    ebnb.setTenant(tenant);
                }
            });
            netSocket.handler(parser::handle);

        }).listen();
    }

    @Override
    public void stop() throws Exception {

    }

}
