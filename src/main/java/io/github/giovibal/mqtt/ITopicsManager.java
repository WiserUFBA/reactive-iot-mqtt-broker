package io.github.giovibal.mqtt;

import java.util.Set;

/**
 * Created by giova_000 on 04/03/2015.
 */
public interface ITopicsManager {

    void addSubscribedTopic(String topic);

    @Deprecated
    Set<String> calculateTopicsToPublish(String topicOfPublishMessage);


    Set<String> getSubscribedTopics();

    @Deprecated
    boolean match(String topic, String topicFilter);

//        public SubscriptionTopic createSubscriptionTopic(String topic);

    void removeSubscribedTopic(String topic);


    String toVertxTopic(String mqttTopic);

}
