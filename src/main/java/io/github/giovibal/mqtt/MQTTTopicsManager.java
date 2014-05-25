package io.github.giovibal.mqtt;

import org.vertx.java.core.Vertx;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by giovanni on 10/05/2014.
 */
public class MQTTTopicsManager {

    private Vertx vertx;
    private Set<String> topicsSubscribed;
    public MQTTTopicsManager(Vertx vertx) {
        this.vertx = vertx;
        this.topicsSubscribed = this.vertx.sharedData().getSet("mqtt_subscribed_topics");
    }

    public Set<String> getSubscribedTopics() {
        return topicsSubscribed;
    }
    public void addSubscribedTopic(String topic) {
        topicsSubscribed.add(topic);
    }
    public void removeSubscribedTopic(String topic) {
        topicsSubscribed.remove(topic);
    }


    public Set<String> calculateTopicsToPublish(String topicOfPublishMessage) {
        String topic = topicOfPublishMessage;
        Set<String> subscribedTopics = getSubscribedTopics();
        Set<String> topicsToPublish = new LinkedHashSet<>();
        for (String tsub : subscribedTopics) {
            if(tsub.equals(topic)) {
                topicsToPublish.add(tsub);
            }
            else {
                if (tsub.contains("+") && !tsub.endsWith("#")) {
                    String pattern = toPattern(tsub);
                    int topicSlashCount = countSlash(topic);
                    int tsubSlashCount = countSlash(tsub);
                    if (topicSlashCount == tsubSlashCount) {
                        if (topic.matches(pattern)) {
                            topicsToPublish.add(tsub);
                        }
                    }
                } else if (tsub.contains("+") || tsub.endsWith("#")) {
                    String pattern = toPattern(tsub);
                    int topicSlashCount = countSlash(topic);
                    int tsubSlashCount = countSlash(tsub);
                    if (topicSlashCount >= tsubSlashCount) {
                        if (topic.matches(pattern)) {
                            topicsToPublish.add(tsub);
                        }
                    }
                }
            }
        }
        return topicsToPublish;
    }

    private String toPattern(String subscribedTopic) {
        String pattern = subscribedTopic.replaceAll("\\+", ".+?");
        pattern = pattern.replaceAll("/#", "/.+");
        return pattern;
    }
    private int countSlash(String s) {
        int count = s.replaceAll("[^/]", "").length();
        return count;
    }
}
