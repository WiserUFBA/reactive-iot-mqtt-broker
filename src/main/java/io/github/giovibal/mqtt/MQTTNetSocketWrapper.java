package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

/**
 * Created by giova_000 on 29/06/2015.
 */
public class MQTTNetSocketWrapper extends NetSocketWrapper {

    private MQTTEncoder encoder = new MQTTEncoder();

    public MQTTNetSocketWrapper(NetSocket netSocket) {
        super(netSocket);
    }

    public void sendMessageToClient(AbstractMessage message) {
        try {
            Buffer b1 = encoder.enc(message);
            sendMessageToClient(b1);
        } catch(Throwable e) {
            Container.logger().error(e.getMessage(), e);
        }
    }
}
