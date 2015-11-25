package io.github.giovibal.mqtt;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;

/**
 * Created by giovanni on 07/05/2014.
 */
public class MQTTWebSocket extends MQTTSocket {

    private ServerWebSocket netSocket;

    public MQTTWebSocket(Vertx vertx, ConfigParser config, ServerWebSocket netSocket) {
        super(vertx, config);
        this.netSocket = netSocket;
    }

    public void start() {
        netSocket.handler(this);
        netSocket.exceptionHandler(event -> {
            String clientInfo = getClientInfo();
            Container.logger().info(clientInfo + ", web-socket closed ... " + netSocket.binaryHandlerID() + " error: " + event.getMessage());
            handleWillMessage();
            shutdown();
        });
        netSocket.closeHandler(aVoid -> {
            String clientInfo = getClientInfo();
            Container.logger().info(clientInfo + ", web-socket closed ... "+ netSocket.binaryHandlerID() +" "+ netSocket.textHandlerID());
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
        Container.logger().debug("web-socket will be closed ... " + netSocket.binaryHandlerID() + " " + netSocket.textHandlerID());
        if(session!=null) {
            session.handleWillMessage();
        }
        netSocket.close();
    }
}
