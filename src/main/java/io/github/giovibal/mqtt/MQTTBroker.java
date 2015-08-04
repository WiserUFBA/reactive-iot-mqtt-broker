package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.persistence.StoreVerticle;
import io.github.giovibal.mqtt.security.AuthorizationVerticle;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Starter;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.PemKeyCertOptions;

/**
 * Created by giovanni on 11/04/2014.
 * The Main Verticle
 */
public class MQTTBroker extends AbstractVerticle {


    public static void main(String[] args) {
        start(args);
    }
    public static void start(String[] args) {
        Starter.main(args);
    }
    public static void stop(String[] args) {
        System.exit(0);
    }


    private void deployVerticle(Class c, DeploymentOptions opt) {
        vertx.deployVerticle(c.getName(), opt,
                result -> {
                    if (result.failed()) {
                        result.cause().printStackTrace();
                    } else {
                        String deploymentID = result.result();
                        Container.logger().debug(c.getSimpleName() + ": " + deploymentID);
                    }
                }
        );
    }
    private void deployAuthorizationWorker(JsonObject config, int instances) {
        deployVerticle(AuthorizationVerticle.class,
                new DeploymentOptions().setWorker(true).setInstances(instances).setConfig(config)
        );
    }
    private void deployStoreVerticle(int instances) {
        deployVerticle(StoreVerticle.class,
                new DeploymentOptions().setWorker(false).setInstances(instances)
        );
    }

    private void deployBridgeServerVerticle(JsonObject config, int instances) {
        deployVerticle(EventBusBridgeServerVerticle.class,
                new DeploymentOptions().setWorker(false).setInstances(instances).setConfig(config)
        );
    }
    private void deployBridgeClientVerticle(JsonObject config, int instances) {
        deployVerticle(EventBusBridgeClientVerticle.class,
                new DeploymentOptions().setWorker(false).setInstances(instances).setConfig(config)
        );
    }

    @Override
    public void stop() {
    }


    @Override
    public void start() {
        try {
            JsonObject config = config();

            // 1 store for 1 broker
            deployStoreVerticle(1);

            // 2 bridge server
            if(config.containsKey("bridge_server")) {
                JsonObject bridgeServerConf = config.getJsonObject("bridge_server", new JsonObject());
                deployBridgeServerVerticle(bridgeServerConf, 1);
            }

            // 3 bridge client
            if(config.containsKey("bridge_client")) {
                JsonObject bridgeClientConf = config.getJsonObject("bridge_client", new JsonObject());
                deployBridgeClientVerticle(bridgeClientConf, 1);
            }

            JsonArray brokers = config.getJsonArray("brokers");
            for(int i=0; i<brokers.size(); i++) {
                JsonObject brokerConf = brokers.getJsonObject(i);

                ConfigParser c = new ConfigParser(brokerConf);
                boolean wsEnabled = c.isWsEnabled();
                boolean securityEnabled = c.isSecurityEnabled();

                if(securityEnabled) {
                    // 2 auth for 1 broker-endpoint-conf that need an authenticator
                    deployAuthorizationWorker(brokerConf, 1);
                }

                // MQTT over WebSocket
                if (wsEnabled) {
                    startWebsocketServer(c);
                }
                else {
                    startTcpServer(c);
                }
                Container.logger().info(
                        "Startd Broker ==> [port: " + c.getPort() + "]" +
                                " [" + c.getFeatursInfo() + "] "
                );
            }

        } catch(Exception e ) {
            Container.logger().error(e.getMessage(), e);
        }

    }



    private void startTcpServer(ConfigParser c) {
        int port = c.getPort();
        String keyPath = c.getTlsKeyPath();
        String certPath = c.getTlsCertPath();
        boolean tlsEnabled = c.isTlsEnabled();

        // MQTT over TCP
        NetServerOptions opt = new NetServerOptions()
                .setTcpKeepAlive(true)
                .setPort(port);

        if(tlsEnabled) {
            opt.setSsl(true).setPemKeyCertOptions(new PemKeyCertOptions()
                            .setKeyPath(keyPath)
                            .setCertPath(certPath)
            )
//              .setClientAuthRequired(true)
//              .setPemTrustOptions(new PemTrustOptions()
//                  .addCertPath("C:\\Sviluppo\\Certificati-SSL\\CA\\rootCA.pem")
//              )
            ;
        }
        NetServer netServer = vertx.createNetServer(opt);
        netServer.connectHandler(netSocket -> {
            MQTTNetSocket mqttNetSocket = new MQTTNetSocket(vertx, c, netSocket);
            mqttNetSocket.start();
        }).listen();
    }

    private void startWebsocketServer(ConfigParser c) {
        int port = c.getPort();
        String wsSubProtocols = c.getWsSubProtocols();
        String keyPath = c.getTlsKeyPath();
        String certPath = c.getTlsCertPath();
        boolean tlsEnabled = c.isTlsEnabled();

        HttpServerOptions httpOpt = new HttpServerOptions()
                .setTcpKeepAlive(true)
//                .setMaxWebsocketFrameSize(1024)
                .setWebsocketSubProtocol(wsSubProtocols)
                .setPort(port);
        if(tlsEnabled) {
            httpOpt.setSsl(true).setPemKeyCertOptions(new PemKeyCertOptions()
                            .setKeyPath(keyPath)
                            .setCertPath(certPath)
            );
        }
        HttpServer http = vertx.createHttpServer(httpOpt);
        http.websocketHandler(serverWebSocket -> {
            MQTTWebSocket mqttWebSocket = new MQTTWebSocket(vertx, c, serverWebSocket);
            mqttWebSocket.start();
        }).listen();
    }
}
