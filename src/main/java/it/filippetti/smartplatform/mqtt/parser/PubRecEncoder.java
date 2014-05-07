package it.filippetti.smartplatform.mqtt.parser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.PubRecMessage;

/**
 *
 * @author andrea
 */
class PubRecEncoder extends DemuxEncoder<PubRecMessage> {

    @Override
    protected void encode(PubRecMessage msg, ByteBuf out) {
        out.writeByte(AbstractMessage.PUBREC << 4);
        out.writeBytes(Utils.encodeRemainingLength(2));
        out.writeShort(msg.getMessageID());
    }
}