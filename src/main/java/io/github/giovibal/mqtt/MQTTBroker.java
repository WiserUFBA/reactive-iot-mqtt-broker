package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.persistence.StoreVerticle;
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
                    Container.logger().info(c.getSimpleName() + ": " + deploymentID);
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


    @Override
    public void stop() {
    }


    @Override
    public void start() {
        try {
            // 1 store for 1 broker
            deployStoreVerticle(1);

            JsonObject config = config();
            JsonArray brokers = config.getJsonArray("brokers");
            for(int i=0; i<brokers.size(); i++) {
                JsonObject brokerConf = brokers.getJsonObject(i);

                ConfigParser c = new ConfigParser(brokerConf);
                boolean wsEnabled = c.isWsEnabled();
                boolean securityEnabled = c.isSecurityEnabled();

                if(securityEnabled) {
                    // 2 auth for 1 broker-endpoint-conf that need an authenticator
                    deployAuthorizationWorker(brokerConf, 2);
                }

                // MQTT over WebSocket
                if (wsEnabled) {
                    startWebsocketServer(c);
                }
                else {
                    startTcpServer(c);
                }
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
//                        .setClientAuthRequired(true)
//                        .setPemTrustOptions(new PemTrustOptions()
//                            .addCertPath("C:\\Sviluppo\\Certificati-SSL\\CA\\rootCA.pem")
//                        )
            ;
        }
        NetServer netServer = vertx.createNetServer(opt);
        netServer.connectHandler(netSocket -> {
            MQTTNetSocket mqttNetSocket = new MQTTNetSocket(vertx, c, netSocket);
            mqttNetSocket.start();
        }).listen();
        Container.logger().info("Startd MQTT TCP-Broker on port: " + port);
    }

    private void startWebsocketServer(ConfigParser c) {
        int port = c.getPort();
        String wsSubProtocols = c.getWsSubProtocols();
        String keyPath = c.getTlsKeyPath();
        String certPath = c.getTlsCertPath();
        boolean tlsEnabled = c.isTlsEnabled();

        HttpServerOptions httpOpt = new HttpServerOptions()
                .setTcpKeepAlive(true)
                .setMaxWebsocketFrameSize(1024)
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
        Container.logger().info("Startd MQTT WebSocket-Broker on port: " + port);
    }
}
