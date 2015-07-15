package io.github.giovibal.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;

/**
 * Created by giova_000 on 15/07/2015.
 */
public class EventBusBridgeServerVerticle extends AbstractVerticle {
    @Override
    public void start() throws Exception {

        JsonObject conf = config();

        String address = MQTTSession.ADDRESS;
        Integer localBridgePort = conf.getInteger("local_bridge_port", 7007);


        // [TCP -> BUS] listen TCP publish to BUS
        NetServerOptions opt = new NetServerOptions()
                .setTcpKeepAlive(true)
                .setPort(localBridgePort);
        NetServer netServer = vertx.createNetServer(opt);
        netServer.connectHandler(netSocket -> {
            netSocket.closeHandler(aVoid -> {
                Container.logger().error("Bridge Server - closed connection from client"+ netSocket.writeHandlerID());
            });
            netSocket.exceptionHandler(throwable -> {
                Container.logger().error("Bridge Server - Exception: "+ throwable.getMessage(), throwable);
            });
            new EventBusNetBridge(netSocket, vertx.eventBus(), address).start();
        }).listen();
    }

    @Override
    public void stop() throws Exception {

    }
}
