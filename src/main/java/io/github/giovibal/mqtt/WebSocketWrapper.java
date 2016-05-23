package io.github.giovibal.mqtt;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketBase;

/**
 * Created by giova_000 on 29/06/2015.
 */
public class WebSocketWrapper {

    private WebSocketBase webSocket;

    public WebSocketWrapper(WebSocketBase netSocket) {
        if(netSocket==null)
            throw new IllegalArgumentException("MQTTWebSocketWrapper: webSocket cannot be null");
        this.webSocket = netSocket;
    }

    // TODO: this method is equals to MQTTWebSocket.sendMessageToClient... need refactoring
    public void sendMessageToClient(Buffer bytes) {
        try {
            webSocket.write(bytes);
            if (webSocket.writeQueueFull()) {
                webSocket.pause();
                webSocket.drainHandler(done -> webSocket.resume() );
            }
        } catch(Throwable e) {
            Container.logger().error(e.getMessage());
        }
    }

    public void stop() {
        // stop writing to socket
        webSocket.drainHandler(null);
    }
}
