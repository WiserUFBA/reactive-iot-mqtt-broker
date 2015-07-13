package io.github.giovibal.mqtt;

import io.vertx.core.Vertx;
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
//        netSocket.setWriteQueueMaxSize(1000);
        netSocket.handler(this);
        netSocket.closeHandler(aVoid -> {
            String clientInfo = getClientInfo();
            Container.logger().debug(clientInfo + ", net-socket closed ... " + netSocket.writeHandlerID());
            handleWillMessage();
            shutdown();
        });
    }

    @Override
    protected void sendMessageToClient(Buffer bytes) {
        try {
            netSocket.write(bytes);
            if (netSocket.writeQueueFull()) {
                netSocket.pause();
                netSocket.drainHandler( done -> netSocket.resume() );
            }
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    protected void closeConnection() {
        Container.logger().debug("net-socket will be closed ... " + netSocket.writeHandlerID());
        netSocket.close();
    }

}
