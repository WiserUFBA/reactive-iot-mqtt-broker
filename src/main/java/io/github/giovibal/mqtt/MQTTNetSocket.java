package io.github.giovibal.mqtt;

import io.vertx.core.Vertx;
import io.vertx.core.VoidHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

/**
 * Created by giovanni on 07/05/2014.
 */
public class MQTTNetSocket extends MQTTSocket {

    private NetSocket netSocket;

    public MQTTNetSocket(Vertx vertx, ConfigParser config, NetSocket netSocket) {
        super(vertx, config);
        this.netSocket = netSocket;
    }

    public void start() {
        netSocket.handler(this);
        netSocket.closeHandler(aVoid -> {
            Container.logger().info("net-socket closed ... "+ netSocket.writeHandlerID());
            shutdown();
        });
    }

    @Override
    protected void sendMessageToClient(Buffer bytes) {
        try {
            if (!netSocket.writeQueueFull()) {
                netSocket.write(bytes);
            } else {
                netSocket.pause();
                netSocket.drainHandler(new VoidHandler() {
                    public void handle() {
                        netSocket.resume();
                    }
                });
            }
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    protected void closeConnection() {
        Container.logger().info("net-socket will be closed ... "+ netSocket.writeHandlerID());
        netSocket.close();
    }
}
