package io.github.giovibal.mqtt;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VoidHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;

/**
 * Created by giovanni on 07/05/2014.
 */
public class MQTTWebSocket extends MQTTSocket {

    private ServerWebSocket netSocket;

    public MQTTWebSocket(Vertx vertx, ServerWebSocket netSocket) {
        super(vertx);
        this.netSocket = netSocket;
    }

    public void start() {
        netSocket.handler(this);
        netSocket.closeHandler(new Handler<Void>() {
            @Override
            public void handle(Void aVoid) {
                Container.logger().info("web-socket closed ... "+ netSocket.binaryHandlerID() +" "+ netSocket.textHandlerID());
                shutdown();
            }
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

}
