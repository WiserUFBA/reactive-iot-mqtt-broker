package io.github.giovibal.mqtt.bridge;

import io.github.giovibal.mqtt.Container;
import io.github.giovibal.mqtt.MQTTSession;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;

/**
 * Created by giova_000 on 15/07/2015.
 */
public class EventBusBridgeWebsocketServerVerticle extends AbstractVerticle {

//    private String tenant;

    @Override
    public void start() throws Exception {

        JsonObject conf = config();

        String address = MQTTSession.ADDRESS;
        Integer localBridgePort = conf.getInteger("local_bridge_port", 7007);

        // [TCP -> BUS] listen TCP publish to BUS
//        NetServerOptions opt = new NetServerOptions()
//                .setTcpKeepAlive(true)
//                .setPort(localBridgePort)
////                .setSsl(true)
////                .setPemKeyCertOptions(new PemKeyCertOptions()
////                    .setKeyPath("C:\\Sviluppo\\Certificati-SSL\\device1\\device1.key")
////                    .setCertPath("C:\\Sviluppo\\Certificati-SSL\\device1\\device1.crt")
////                )
////                .setClientAuthRequired(true)
////                .setPemTrustOptions(new PemTrustOptions()
////                    .addCertPath("C:\\Sviluppo\\Certificati-SSL\\CA\\rootCA.pem")
////                )
//            ;
//        NetServer netServer = vertx.createNetServer(opt);

        HttpServerOptions opt = new HttpServerOptions()
                .setTcpKeepAlive(true)
                .setPort(localBridgePort)
        ;

        HttpServer netServer = vertx.createHttpServer(opt);
        netServer.websocketHandler(webSocket -> {
            if (!webSocket.path().equals("/bridge")) {
                webSocket.reject();
            }
            else {
                final EventBusWebsocketBridge ebnb = new EventBusWebsocketBridge(webSocket, vertx.eventBus(), address);
                webSocket.closeHandler(aVoid -> {
                    Container.logger().info("Bridge Server - closed connection from client " + webSocket.textHandlerID());
                    ebnb.stop();
                });
                webSocket.exceptionHandler(throwable -> {
                    Container.logger().error("Bridge Server - Exception: " + throwable.getMessage(), throwable);
                    ebnb.stop();
                });

                Container.logger().info("Bridge Server - new connection from client " + webSocket.textHandlerID());
                //            // TODO: some sort of authentication with tenant
                //            try {
                //                X509Certificate[] certs = webSocket.peerCertificateChain();
                //                for(X509Certificate c : certs) {
                //                    String dn = c.getSubjectDN().getName();// info del DEVICE/TENANT
                //                    try {
                //                        LdapName ldapDN = new LdapName(dn);
                //                        for (Rdn rdn : ldapDN.getRdns()) {
                //                            System.out.println(rdn.getType() + " -> " + rdn.getValue());
                //                            if(rdn.getType().equals("CN")) {
                //                                tenant = rdn.getValue().toString();
                //                            }
                //                        }
                //                    } catch (InvalidNameException in) {
                //                        in.printStackTrace();
                //                    }
                //                }
                //            } catch (SSLPeerUnverifiedException e) {
                //                e.printStackTrace();
                //            }

                final RecordParser parser = RecordParser.newDelimited("\n", h -> {
                    String cmd = h.toString();
                    if ("START SESSION".equalsIgnoreCase(cmd)) {
                        webSocket.pause();
                        Container.logger().info("Bridge Server - start session with tenant: " + ebnb.getTenant());
                        //                    new EventBusNetBridge(webSocket, vertx.eventBus(), address, tenant).start();
                        //                    ebnb = new EventBusNetBridge(webSocket, vertx.eventBus(), address, tenant);
                        ebnb.start();
                        Container.logger().info("Bridge Server - bridgeUUID: " + ebnb.getBridgeUUID());
                        webSocket.resume();
                    } else {
                        String tenant = cmd;
                        ebnb.setTenant(tenant);
                    }
                });
                webSocket.handler(parser::handle);
            }

        }).listen();
    }

    @Override
    public void stop() throws Exception {

    }
}
