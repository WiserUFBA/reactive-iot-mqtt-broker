package io.github.giovibal.mqtt.parser;

import org.dna.mqtt.moquette.proto.messages.MessageIDMessage;
import org.dna.mqtt.moquette.proto.messages.PubRelMessage;

/**
 *
 * @author andrea
 */
class PubRelDecoder extends MessageIDDecoder {

    @Override
    protected MessageIDMessage createMessage() {
        return new PubRelMessage();
    }

}

