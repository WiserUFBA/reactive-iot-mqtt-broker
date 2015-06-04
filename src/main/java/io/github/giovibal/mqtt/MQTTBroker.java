package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.persistence.StoreVerticle;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.impl.Deployment;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;

import java.util.Set;

/**
 * Created by giovanni on 11/04/2014.
 * The Main Verticle
 */
public class MQTTBroker extends AbstractVerticle {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        //TODO: capire come utilizzare la configurazione
        int instances = Runtime.getRuntime().availableProcessors();
        if(instances > 2) {
            instances = instances - 2;
        }

                // broker
        vertx.deployVerticle(new MQTTBroker(), new DeploymentOptions().setInstances(instances),
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
            // deplying authorization worker verticle ...
            vertx.deployVerticle(AuthorizationVerticle.class.getName(), new DeploymentOptions().setWorker(true).setInstances(10),
                result -> {
                    if (result.failed()) {
                        result.cause().printStackTrace();
                    } else {
                        System.out.println(AuthorizationVerticle.class.getSimpleName()+": "+result.result());
                    }
                }
            );
            vertx.deployVerticle(StoreVerticle.class.getName(), new DeploymentOptions().setWorker(false).setInstances(1),
                result -> {
                    if (result.failed()) {
                        result.cause().printStackTrace();
                    } else {
                        System.out.println(StoreVerticle.class.getSimpleName()+": "+result.result());
                    }
                }
            );

            JsonObject config = config();
            JsonArray brokers = config.getJsonArray("brokers");
            for(int i=0; i<brokers.size(); i++) {
                JsonObject brokerConf = brokers.getJsonObject(i);


                ConfigParser c = new ConfigParser(brokerConf);
                int port = c.getPort();
                int wsPort = c.getWsPort();
                boolean wsEnabled = c.isWsEnabled();
                String wsSubProtocols = c.getWsSubProtocols();
                boolean securityEnabled = c.isSecurityEnabled();

                // MQTT over TCP
                NetServerOptions opt = new NetServerOptions()
                        .setTcpKeepAlive(true)
                        .setPort(port);
                // SSL setup

                String keyPath = c.getTlsKeyPath();
                String certPath = c.getTlsCertPath();
                boolean tlsEnabled = c.isTlsEnabled();
                if(tlsEnabled) {
                    opt.setSsl(true).setPemKeyCertOptions(new PemKeyCertOptions()
                                    .setKeyPath(keyPath)
                                    .setCertPath(certPath)
                    )
//                        .setClientAuthRequired(true)
//                        .setPemTrustOptions(new PemTrustOptions()
//                            .addCertPath("C:\\Sviluppo\\Certificati-SSL\\CA\\rootCA.pem")
//                        )
                    ;
                }
                NetServer netServer = vertx.createNetServer(opt);
                netServer.connectHandler(netSocket -> {
                    Container.logger().info("IS SSL: " + netSocket.isSsl());
                    MQTTNetSocket mqttNetSocket = new MQTTNetSocket(vertx, c, netSocket);
                    mqttNetSocket.start();
                }).listen();
                Container.logger().info("Startd MQTT TCP-Broker on port: " + port);

                // MQTT over WebSocket
                if (wsEnabled) {
                    HttpServerOptions httpOpt = new HttpServerOptions()
                        .setTcpKeepAlive(true)
                        .setMaxWebsocketFrameSize(1024)
                        .setWebsocketSubProtocol(wsSubProtocols)
                        .setPort(wsPort);
                    if(tlsEnabled) {
                        httpOpt.setSsl(true).setPemKeyCertOptions(new PemKeyCertOptions()
                            .setKeyPath(keyPath)
                            .setCertPath(certPath)
                        );
                    }

                    final HttpServer http = vertx.createHttpServer(httpOpt);
                    http.websocketHandler(serverWebSocket -> {
                        MQTTWebSocket mqttWebSocket = new MQTTWebSocket(vertx, c, serverWebSocket);
                        mqttWebSocket.start();
                    }).listen();
                    Container.logger().info("Startd MQTT WebSocket-Broker on port: " + wsPort);
                }
            }

        } catch(Exception e ) {
            Container.logger().error(e.getMessage(), e);
        }

    }

    @Override
    public void stop() {
    }

}
