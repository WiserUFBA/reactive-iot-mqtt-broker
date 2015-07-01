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
public class RevProxyFrontendVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {

        // PROXY FRONTEND
        int proxyPort = config().getInteger("proxy.frontend.port", 1885);

        NetServer netServer = vertx.createNetServer(new NetServerOptions().setPort(proxyPort));
        netServer.connectHandler(proxyNetSocket -> {
            NetSocketWrapper proxySocket = new NetSocketWrapper(proxyNetSocket);
            proxyNetSocket.handler(buffer -> {
                Container.logger().info("MQTT Proxy from-proxy-to-backend");
                vertx.eventBus().send("from-proxy-to-backend", buffer);
            });
            vertx.eventBus().consumer("from-backend-to-proxy", (Message<Buffer> objectMessage) -> {
                Container.logger().info("MQTT Proxy from-backend-to-proxy");
                Buffer buff = objectMessage.body();
                proxySocket.sendMessageToClient(buff);
            });
        });
        netServer.listen();


    }

}
