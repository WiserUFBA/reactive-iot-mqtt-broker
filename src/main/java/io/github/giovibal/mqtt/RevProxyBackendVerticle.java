package io.github.giovibal.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;

/**
 * Created by giova_000 on 29/06/2015.
 */
public class RevProxyBackendVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {

        // PROXY BACKEND
        int backendPort = config().getInteger("proxy.backend.port", 1884);
        String backendHost = config().getString("proxy.backend.host","192.168.231.53");

        NetClient netClient = vertx.createNetClient();
        netClient.connect(backendPort, backendHost, netSocketAsyncResult -> {
            if (netSocketAsyncResult.succeeded()) {
                NetSocket backendNetSocket = netSocketAsyncResult.result();
                NetSocketWrapper backendSocket = new MQTTNetSocketWrapper(backendNetSocket);
                backendNetSocket.handler(buffer -> {
                    Container.logger().info("MQTT Backend from-backend-to-proxy");
                    vertx.eventBus().send("from-backend-to-proxy", buffer);
                });
                vertx.eventBus().consumer("from-proxy-to-backend", (Message<Buffer> objectMessage) -> {
                    Container.logger().info("MQTT Backend from-proxy-to-backend");
                    Buffer buff = objectMessage.body();
                    backendSocket.sendMessageToClient(buff);
                });
            }
        });


    }

}
