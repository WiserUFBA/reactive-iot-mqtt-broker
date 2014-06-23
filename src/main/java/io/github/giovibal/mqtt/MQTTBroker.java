package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.persistence.MQTTStoreManager;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.ServerWebSocket;
import org.vertx.java.core.http.WebSocketFrame;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.streams.Pump;
import org.vertx.java.platform.Verticle;

import java.util.Set;

/**
 * Created by giovanni on 11/04/2014.
 * The Main Verticle
 */
public class MQTTBroker extends Verticle {

    @Override
    public void start() {
        try {

            NetServer netServer = vertx.createNetServer();
            netServer.connectHandler(new Handler<NetSocket>() {
                @Override
                public void handle(final NetSocket netSocket) {
                    MQTTNetSocket mqttNetSocket = new MQTTNetSocket(vertx, container, netSocket);
                    mqttNetSocket.start();
                }
            }).listen(1883);
            container.logger().info("Startd MQTT TCP-Broker on port: "+ 1883);

            final HttpServer http = vertx.createHttpServer();
            http.setWebSocketSubProtocols("mqttv3.1");
            http.setMaxWebSocketFrameSize(1024);
            http.websocketHandler(new Handler<ServerWebSocket>() {
                @Override
                public void handle(ServerWebSocket serverWebSocket) {
                    MQTTWebSocket mqttWebSocket = new MQTTWebSocket(vertx, container, serverWebSocket);
                    mqttWebSocket.start();
                }
            }).listen(8000);
            container.logger().info("Startd MQTT WebSocket-Broker on port: "+ 8000);


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
            container.logger().error(e.getMessage(), e);
        }

    }

    @Override
    public void stop() {
    }

}
