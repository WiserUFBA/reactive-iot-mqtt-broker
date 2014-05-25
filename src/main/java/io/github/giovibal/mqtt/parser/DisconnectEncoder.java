package io.github.giovibal.mqtt.parser;

import io.netty.buffer.ByteBuf;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.DisconnectMessage;

/**
 *
 * @author andrea
 */
public class DisconnectEncoder extends DemuxEncoder<DisconnectMessage> {

    @Override
    protected void encode(DisconnectMessage msg, ByteBuf out) {
        out.writeByte(AbstractMessage.DISCONNECT << 4).writeByte(0);
    }
    
}
