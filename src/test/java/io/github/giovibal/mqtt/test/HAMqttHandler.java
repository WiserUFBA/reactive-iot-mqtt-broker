package io.github.giovibal.mqtt.test;

import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by giovanni on 24/05/2014.
 */
public class HAMqttHandler extends DefaultMqttHandler {
    private Map<String, Integer> countByTopic = new LinkedHashMap<>();

    private void incrementMessagesCount(String topic) {
        Integer count = 0;
        if(countByTopic.containsKey(topic)) {
            count = countByTopic.get(topic);
        }
        count = count +1;
        countByTopic.put(topic, count);
    }

    public Integer countMessagesByTopic(String topic) {
        if(countByTopic.containsKey(topic))
            return countByTopic.get(topic);
        return 0;
    }
    public boolean countMessagesByTopicEquals(String topic, int count) {
        return countMessagesByTopic(topic) == count;
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        System.out.println(topic +" ==> "+ new String(mqttMessage.getPayload(), "UTF-8"));
        incrementMessagesCount(topic);
    }

}
