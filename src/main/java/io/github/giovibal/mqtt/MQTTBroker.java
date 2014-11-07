package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.persistence.MQTTStoreManager;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.ServerWebSocket;
import org.vertx.java.core.http.WebSocketFrame;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
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
            JsonObject conf = container.config();
            int port = conf.getInteger("tcp_port", 1883);
            int wsPort = conf.getInteger("websocket_port", 11883);
            boolean wsEnabled = conf.getBoolean("websocket_enabled", true);
            String wsSubProtocols = conf.getString("websocket_subprotocols", "mqtt,mqttv3.1");
            String[] wsSubProtocolsArr = wsSubProtocols.split(",");

            // MQTT over TCP
            NetServer netServer = vertx.createNetServer();
            netServer.setTCPKeepAlive(true);
            netServer.connectHandler(new Handler<NetSocket>() {
                @Override
                public void handle(final NetSocket netSocket) {
                    MQTTNetSocket mqttNetSocket = new MQTTNetSocket(vertx, container, netSocket);
                    mqttNetSocket.start();
                }
            }).listen(port);
            container.logger().info("Startd MQTT TCP-Broker on port: "+ port);

            // MQTT over WebSocket
            if(wsEnabled) {
                final HttpServer http = vertx.createHttpServer();
                http.setWebSocketSubProtocols(wsSubProtocolsArr);
                http.setTCPKeepAlive(true);
                http.setMaxWebSocketFrameSize(1024);
                http.websocketHandler(new Handler<ServerWebSocket>() {
                    @Override
                    public void handle(ServerWebSocket serverWebSocket) {
                        MQTTWebSocket mqttWebSocket = new MQTTWebSocket(vertx, container, serverWebSocket);
                        mqttWebSocket.start();
                    }
                }).listen(wsPort);
                container.logger().info("Startd MQTT WebSocket-Broker on port: " + wsPort);
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
            container.logger().error(e.getMessage(), e);
        }

    }

    @Override
    public void stop() {
    }

}
