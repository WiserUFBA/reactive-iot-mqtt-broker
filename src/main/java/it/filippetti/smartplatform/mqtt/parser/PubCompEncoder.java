package it.filippetti.smartplatform.mqtt.parser;

import io.netty.buffer.ByteBuf;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.PubCompMessage;

/**
 *
 * @author andrea
 */
class PubCompEncoder extends DemuxEncoder<PubCompMessage> {

    @Override
    protected void encode(PubCompMessage msg, ByteBuf out) {
        out.writeByte(AbstractMessage.PUBCOMP << 4);
        out.writeBytes(Utils.encodeRemainingLength(2));
        out.writeShort(msg.getMessageID());
    }
}
