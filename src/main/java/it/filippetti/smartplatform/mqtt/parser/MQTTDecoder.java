package it.filippetti.smartplatform.mqtt.parser;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.CorruptedFrameException;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author andrea
 */
public class MQTTDecoder {
    
    private Map<Byte, DemuxDecoder> m_decoderMap = new HashMap<Byte, DemuxDecoder>();
    
    public MQTTDecoder() {
       m_decoderMap.put(AbstractMessage.CONNECT, new ConnectDecoder());
       m_decoderMap.put(AbstractMessage.CONNACK, new ConnAckDecoder());
       m_decoderMap.put(AbstractMessage.PUBLISH, new PublishDecoder());
       m_decoderMap.put(AbstractMessage.PUBACK, new PubAckDecoder());
       m_decoderMap.put(AbstractMessage.SUBSCRIBE, new SubscribeDecoder());
       m_decoderMap.put(AbstractMessage.SUBACK, new SubAckDecoder());
       m_decoderMap.put(AbstractMessage.UNSUBSCRIBE, new UnsubscribeDecoder());
       m_decoderMap.put(AbstractMessage.DISCONNECT, new DisconnectDecoder());
       m_decoderMap.put(AbstractMessage.PINGREQ, new PingReqDecoder());
       m_decoderMap.put(AbstractMessage.PINGRESP, new PingRespDecoder());
       m_decoderMap.put(AbstractMessage.UNSUBACK, new UnsubAckDecoder());
       m_decoderMap.put(AbstractMessage.PUBCOMP, new PubCompDecoder());
       m_decoderMap.put(AbstractMessage.PUBREC, new PubRecDecoder());
       m_decoderMap.put(AbstractMessage.PUBREL, new PubRelDecoder());
    }

    public void decode(ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        if (!checkHeaderAvailability(in)) {
            in.resetReaderIndex();
            return;
        }
        in.resetReaderIndex();
        
        byte messageType = readMessageType(in);
        
        DemuxDecoder decoder = m_decoderMap.get(messageType);
        if (decoder == null) {
            throw new CorruptedFrameException("Can't find any suitable decoder for message type: " + messageType);
        }
        decoder.decode(in, out);
    }

    private byte readMessageType(ByteBuf in) {
        byte h1 = in.readByte();
        byte messageType = (byte) ((h1 & 0x00F0) >> 4);
        return messageType;
    }
    private boolean checkHeaderAvailability(ByteBuf in) {
        if (in.readableBytes() < 1) {
            return false;
        }
        //byte h1 = in.get();
        //byte messageType = (byte) ((h1 & 0x00F0) >> 4);
        in.skipBytes(1); //skip the messageType byte

        int remainingLength = Utils.decodeRemainingLenght(in);
        if (remainingLength == -1) {
            return false;
        }

        //check remaining length
        if (in.readableBytes() < remainingLength) {
            return false;
        }

        //return messageType == type ? MessageDecoderResult.OK : MessageDecoderResult.NOT_OK;
        return true;
    }
}
