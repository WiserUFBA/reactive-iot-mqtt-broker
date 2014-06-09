package io.github.giovibal.mqtt;

import org.vertx.java.core.Vertx;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by giovanni on 10/05/2014.
 * Manages subscritpions and MQTT topic rules
 */
public class MQTTTopicsManager {

    private Vertx vertx;
    private Map<String, Integer> topicsSubscribed;
    private String tenant;

    public MQTTTopicsManager(Vertx vertx, String tenant) {
        this.vertx = vertx;
        this.tenant = tenant;
        this.topicsSubscribed = this.vertx.sharedData().getMap(this.tenant + "mqtt_subscribed_topics");
    }

    public Set<String> getSubscribedTopics() {
        return topicsSubscribed.keySet();
    }
    public void addSubscribedTopic(String topic) {
        Integer retain = 0;
        if(topicsSubscribed.containsKey(topic)) {
            retain = topicsSubscribed.get(topic);
        }
        retain++;
        topicsSubscribed.put(topic, retain);
    }
    public void removeSubscribedTopic(String topic) {
        Integer retain = 0;
        if(topicsSubscribed.containsKey(topic)) {
            retain = topicsSubscribed.get(topic);
        }
        if(retain <= 0) {
            topicsSubscribed.remove(topic);
        }
        else {
            retain--;
            topicsSubscribed.put(topic, retain);
        }
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
