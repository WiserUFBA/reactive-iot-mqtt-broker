package io.github.giovibal.mqtt;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.ServerWebSocket;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.platform.Verticle;

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
            });

            netServer.listen(1883);
            container.logger().info("Startd MQTT TCP-Broker on port: "+ 1883);

            vertx.createHttpServer().websocketHandler(new Handler<ServerWebSocket>() {
                @Override
                public void handle(ServerWebSocket serverWebSocket) {
                    MQTTWebSocket mqttWebSocket = new MQTTWebSocket(vertx, container, serverWebSocket);
                    mqttWebSocket.start();
                }
            }).listen(11883);
            container.logger().info("Startd MQTT WebSocket-Broker on port: "+ 11883);
        } catch(Exception e ) {
            container.logger().error(e.getMessage(), e);
        }

    }

    @Override
    public void stop() {
    }

}
