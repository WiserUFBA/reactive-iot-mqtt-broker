package io.github.giovibal.mqtt;

import java.util.Set;

/**
 * Created by giova_000 on 04/03/2015.
 */
public interface ITopicsManager {

    void addSubscribedTopic(String topic);

    Set<String> calculateTopicsToPublish(String topicOfPublishMessage);


    Set<String> getSubscribedTopics();

    boolean match(String topic, String topicFilter);

    void removeSubscribedTopic(String topic);


    String toVertxTopic(String mqttTopic);

}
