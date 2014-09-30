package io.github.giovibal.mqtt;

import io.netty.buffer.ByteBuf;
import org.dna.mqtt.moquette.proto.messages.ConnAckMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.ServerWebSocket;
import org.vertx.java.core.http.WebSocketFrame;
import org.vertx.java.core.http.impl.ws.DefaultWebSocketFrame;
import org.vertx.java.core.http.impl.ws.WebSocketFrameInternal;
import org.vertx.java.platform.Container;

/**
 * Created by giovanni on 07/05/2014.
 */
public class MQTTWebSocket extends MQTTSocket {

    private ServerWebSocket netSocket;

    public MQTTWebSocket(Vertx vertx, Container container, ServerWebSocket netSocket) {
        super(vertx, container);
        this.netSocket = netSocket;
    }

    public void start() {
        netSocket.dataHandler(this);
        netSocket.closeHandler(new Handler<Void>() {
            @Override
            public void handle(Void aVoid) {
                container.logger().info("web-socket closed ... "+ netSocket.binaryHandlerID() +" "+ netSocket.textHandlerID());
                shutdown();
            }
        });
    }

    @Override
    protected void sendMessageToClient(Buffer bytes) {
        try {
            if (!netSocket.writeQueueFull()) {
                netSocket.write(bytes);
//                netSocket.writeBinaryFrame(bytes);
            } else {
                netSocket.pause();
                netSocket.drainHandler(new VoidHandler() {
                    public void handle() {
                        netSocket.resume();
                    }
                });
            }
        } catch(Throwable e) {
            container.logger().error(e.getMessage());
        }
    }

}
