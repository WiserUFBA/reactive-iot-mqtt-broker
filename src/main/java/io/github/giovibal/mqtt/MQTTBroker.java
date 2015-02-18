package io.github.giovibal.mqtt;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;

/**
 * Created by giovanni on 11/04/2014.
 * The Main Verticle
 */
public class MQTTBroker extends AbstractVerticle {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        startup(vertx);

//        VertxOptions options = new VertxOptions();
//        options.setClustered(true);
//        options.setClusterPingInterval(1000);
//        Vertx.clusteredVertx(options, res -> {
//            if (res.succeeded()) {
//                Vertx vertx = res.result();
//                EventBus eventBus = vertx.eventBus();
//                System.out.println("We now have a clustered event bus: " + eventBus);
//                startup(vertx);
//            } else {
//                System.out.println("Failed: " + res.cause());
//            }
//        });

    }

    private static void startup(Vertx vertx) {
//        JsonObject conf = vertx.getOrCreateContext().config();
//        System.out.println(conf.getInteger("tcp_port", 1883));
//        System.out.println(conf.getInteger("websocket_port", 11883));
//        System.out.println(conf.getBoolean("websocket_enabled", true));
//        System.out.println(conf.getString("websocket_subprotocols", "mqtt,mqttv3.1"));
        //TODO: capire come utilizzare la configurazione
        int instances = Runtime.getRuntime().availableProcessors();
        if(instances > 2) {
            instances = instances - 2;
        }
//        // autenticator
//        vertx.deployVerticle(new AuthenticatorVerticle(),new DeploymentOptions().setInstances(instances),
//                result -> {
//                    if (result.failed()) {
//                        result.cause().printStackTrace();
//                    } else {
//                        System.out.println(AuthenticatorVerticle.class.getSimpleName()+": "+result.result());
//                    }
//                }
//        );

        // broker
        vertx.deployVerticle(new MQTTBroker(),
                new DeploymentOptions().setInstances(instances),
                result -> {
                    if (result.failed()) {
                        result.cause().printStackTrace();
                    } else {
                        System.out.println(MQTTBroker.class.getSimpleName()+": "+result.result());
                    }
                }
        );
    }

    @Override
    public void start() {
        try {
            JsonObject conf = config();
            int port = conf.getInteger("tcp_port", 1883);
            int wsPort = conf.getInteger("websocket_port", 11883);
            boolean wsEnabled = conf.getBoolean("websocket_enabled", true);
            String wsSubProtocols = conf.getString("websocket_subprotocols", "mqtt,mqttv3.1");
//            String[] wsSubProtocolsArr = wsSubProtocols.split(",");

            // MQTT over TCP
            NetServerOptions opt = new NetServerOptions()
                    .setTcpKeepAlive(true)
                    .setPort(port)
                    .setSsl(true)
                    .setPemKeyCertOptions(new PemKeyCertOptions()
                                    .setKeyPath("C:\\Sviluppo\\Certificati-SSL\\device1\\device1.key")
                                    .setCertPath("C:\\Sviluppo\\Certificati-SSL\\device1\\device1.crt")
                    )
                    .setClientAuthRequired(true)
                    .setPemTrustOptions(new PemTrustOptions()
                                    .addCertPath("C:\\Sviluppo\\Certificati-SSL\\CA\\rootCA.pem")
                    )
                    ;
            NetServer netServer = vertx.createNetServer(opt);
            netServer.connectHandler(netSocket -> {
                Container.logger().info("IS SSL: "+ netSocket.isSsl());
                MQTTNetSocket mqttNetSocket = new MQTTNetSocket(vertx, netSocket);
                mqttNetSocket.start();
            }).listen();
            Container.logger().info("Startd MQTT TCP-Broker on port: "+ port);

            // MQTT over WebSocket
            if(wsEnabled) {
                HttpServerOptions httpOpt = new HttpServerOptions()
                        .setTcpKeepAlive(true)
                        .setMaxWebsocketFrameSize(1024)
                        .setWebsocketSubProtocol(wsSubProtocols)
                        .setPort(wsPort);

                final HttpServer http = vertx.createHttpServer(httpOpt);
                http.websocketHandler(serverWebSocket -> {
                    MQTTWebSocket mqttWebSocket = new MQTTWebSocket(vertx, serverWebSocket);
                    mqttWebSocket.start();
                }).listen();
                Container.logger().info("Startd MQTT WebSocket-Broker on port: " + wsPort);
            }

//            final MQTTStoreManager store = new MQTTStoreManager(vertx, "");
//            // DEBUG
//            vertx.setPeriodic(10000, new Handler<Long>() {
//                @Override
//                public void handle(Long aLong) {
//                    container.logger().info("stats...");
//                    Set<String> clients = store.getClientIDs();
//                    for(String clientID : clients) {
//                        int subscriptions = store.getSubscriptionsByClientID(clientID).size();
//                        container.logger().info(clientID+" ----> "+ subscriptions);
//                    }
//                }
//            });
        } catch(Exception e ) {
            Container.logger().error(e.getMessage(), e);
        }

    }

    @Override
    public void stop() {
    }

}
