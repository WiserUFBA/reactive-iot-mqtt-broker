package it.filippetti.smartplatform.mqtt.parser;

import io.netty.buffer.ByteBuf;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.ConnAckMessage;

/**
 *
 * @author andrea
 */
class ConnAckEncoder extends DemuxEncoder<ConnAckMessage> {

    @Override
    protected void encode(ConnAckMessage message, ByteBuf out) {
        out.writeByte(AbstractMessage.CONNACK << 4);
        out.writeBytes(Utils.encodeRemainingLength(2));
        out.writeByte(0);
        out.writeByte(message.getReturnCode());
    }
    
}
