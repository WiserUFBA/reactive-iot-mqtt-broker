package io.github.giovibal.mqtt.persistence;

import io.github.giovibal.mqtt.ITopicsManager;
import io.github.giovibal.mqtt.MQTTTopicsManagerOptimized;
import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.vertx.core.buffer.Buffer;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by giova_000 on 03/06/2015.
 */
public class StoreManager {

    private ITopicsManager topicsManager;
    private MQTTEncoder encoder;
    private MQTTDecoder decoder;
    private Map<String, byte[]> retained;

    public StoreManager(String tenant) {
        retained = new LinkedHashMap<>();
        topicsManager = new MQTTTopicsManagerOptimized(tenant);
        encoder = new MQTTEncoder();
        decoder = new MQTTDecoder();
    }

    public void saveRetainMessage(PublishMessage pm) {
        try {
            byte[] pmBytes = encoder.enc(pm).getBytes();
            retained.put(pm.getTopicName(), pmBytes);
        } catch(Throwable e) {
            e.printStackTrace();
        }
    }
    public List<PublishMessage> getRetainedMessagesByTopicFilter(String topicFilter) {
        List<PublishMessage> list = new ArrayList<>();
        for(byte[] pmBytes : retained.values()) {
            try {
                PublishMessage pm = (PublishMessage)decoder.dec(Buffer.buffer(pmBytes));
                boolean topicMatch = topicsManager.match(pm.getTopicName(), topicFilter);
                if(topicMatch) {
                    list.add(pm);
                }
            } catch(Throwable e) {
                e.printStackTrace();
            }
        }
        return list;
    }


}
