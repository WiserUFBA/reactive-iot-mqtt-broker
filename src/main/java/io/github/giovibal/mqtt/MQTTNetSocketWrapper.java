package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

/**
 * Created by giova_000 on 29/06/2015.
 */
public class MQTTNetSocketWrapper {

    private MQTTEncoder encoder = new MQTTEncoder();
    private NetSocket netSocket;

    public MQTTNetSocketWrapper(NetSocket netSocket) {
        if(netSocket==null)
            throw new IllegalArgumentException("MQTTNetSocketWrapper: netSocket cannot be null");
        this.netSocket = netSocket;
        netSocket.handler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {

            }
        });
    }

    public void sendMessageToClient(AbstractMessage message) {
        try {
            Buffer b1 = encoder.enc(message);
            sendMessageToClient(b1);
        } catch(Throwable e) {
            Container.logger().error(e.getMessage(), e);
        }
    }
    public void sendMessageToClient(Buffer bytes) {
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
}
