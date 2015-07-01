package io.github.giovibal.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;

/**
 * Created by giova_000 on 29/06/2015.
 */
public class RevProxyBackendVerticle extends AbstractVerticle {

    int backendPort;
    String backendHost;
    String managerTopic;

    @Override
    public void start() throws Exception {

        // PROXY BACKEND
        backendPort = config().getInteger("proxy.backend.port", 1884);
        backendHost = config().getString("proxy.backend.host","192.168.231.53");
        managerTopic = config().getString("manager.topic");

        NetClientOptions opt = new NetClientOptions()
                .setConnectTimeout(5000)
                .setIdleTimeout(1000)
                .setReconnectAttempts(10)
                .setReconnectInterval(500)
                ;
        NetClient netClient = vertx.createNetClient(opt);
        netClient.connect(backendPort, backendHost, netSocketAsyncResult -> {
            if (netSocketAsyncResult.succeeded()) {
                NetSocket backendNetSocket = netSocketAsyncResult.result();
                backendNetSocket.closeHandler(aVoid -> {
                    notifyFailedStatus();
                });

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
            else {
                netSocketAsyncResult.cause().printStackTrace();
                notifyFailedStatus();
            }
        });


    }

    private void notifyFailedStatus() {
        vertx.eventBus().publish(managerTopic, deploymentID());
    }

}
