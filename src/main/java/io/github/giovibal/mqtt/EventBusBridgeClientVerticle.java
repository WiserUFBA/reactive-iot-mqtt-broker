package io.github.giovibal.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.streams.Pump;

/**
 * Created by giova_000 on 15/07/2015.
 */
public class EventBusBridgeClientVerticle extends AbstractVerticle implements Handler<AsyncResult<NetSocket>> {

    private NetClient netClient;
    private String remoteBridgeHost;
    private Integer remoteBridgePort;
    private String address;

    @Override
    public void start() throws Exception {

        JsonObject conf = config();

        remoteBridgeHost = conf.getString("remote_bridge_host", "192.168.231.53");
        remoteBridgePort = conf.getInteger("remote_bridge_port", 7007);
        address = MQTTSession.ADDRESS;

        // [TCP <- BUS] listen BUS write to TCP
        NetClientOptions opt = new NetClientOptions()
                .setConnectTimeout(1000) // 60 seconds
//                .setIdleTimeout(10) // 10 seconds (0 sec default)
                .setTcpKeepAlive(true)
//                .setReconnectInterval(1000) // (1 sec default)
                ;

//        System.out.println(opt.getReconnectAttempts());
//        System.out.println(opt.getReconnectInterval());

        netClient = vertx.createNetClient(opt);
        netClient.connect(remoteBridgePort, remoteBridgeHost, this);
    }

    private long connectionTimerID;

    @Override
    public void handle(AsyncResult<NetSocket> netSocketAsyncResult) {
        if (netSocketAsyncResult.succeeded()) {
            vertx.cancelTimer(connectionTimerID);
            Container.logger().info("Bridge Client - connected to server [" + remoteBridgeHost + ":" + remoteBridgePort +"]");
            NetSocket netSocket = netSocketAsyncResult.result();
            netSocket.closeHandler(aVoid -> {
                Container.logger().error("Bridge Client - closed connection from server" + netSocket.writeHandlerID());
                connectionTimerID = vertx.setPeriodic(1000, aLong -> {
                    netClient.connect(remoteBridgePort, remoteBridgeHost, this);
                });
            });
            netSocket.exceptionHandler(throwable -> {
                Container.logger().error("Bridge Client - Exception: " + throwable.getMessage(), throwable);
            });
            new EventBusNetBridge(netSocket, vertx.eventBus(), address).start();
        } else {
            String msg = "Bridge Client - not connected to server [" + remoteBridgeHost + ":" + remoteBridgePort +"]";
            Throwable e = netSocketAsyncResult.cause();
            if (e != null) {
                Container.logger().error(msg, e);
            } else {
                Container.logger().error(msg);
            }
        }
    }

    @Override
    public void stop() throws Exception {

    }
}
