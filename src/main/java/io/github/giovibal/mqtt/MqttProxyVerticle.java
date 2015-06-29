package io.github.giovibal.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.net.*;
import io.vertx.core.streams.Pump;
import org.dna.mqtt.moquette.proto.messages.ConnectMessage;

/**
 * Created by giova_000 on 29/06/2015.
 */
public class MqttProxyVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {

        // PROXY TEST

        int backendPort = 1884;
        String backendHost = "192.168.231.53";
        int proxyPort = 1885;


        NetServer netServer = vertx.createNetServer(new NetServerOptions().setPort(proxyPort));
        netServer.connectHandler(proxyNetSocket -> {
            proxyNetSocket.handler(buffer -> {
                Container.logger().info("MQTT Proxy from-proxy-to-backend");
                vertx.eventBus().send("from-proxy-to-backend", buffer);
            });
            vertx.eventBus().localConsumer("from-backend-to-proxy", (Message<Buffer> objectMessage) -> {
                Container.logger().info("MQTT Proxy from-backend-to-proxy");
                proxyNetSocket.write(objectMessage.body());
            });
        });
        netServer.listen();


        NetClient netClient = vertx.createNetClient();
        netClient.connect(backendPort, backendHost, netSocketAsyncResult -> {
            if (netSocketAsyncResult.succeeded()) {
                NetSocket backendNetSocket = netSocketAsyncResult.result();
                backendNetSocket.handler(buffer -> {
                    Container.logger().info("MQTT Backend from-backend-to-proxy");
                    vertx.eventBus().send("from-backend-to-proxy", buffer);
                });
                vertx.eventBus().localConsumer("from-proxy-to-backend", (Message<Buffer> objectMessage) -> {
                    Container.logger().info("MQTT Backend from-proxy-to-backend");
                    Buffer buff = objectMessage.body();
                    backendNetSocket.write(buff);
                });
            }
        });


    }

}
