package io.github.giovibal.mqtt;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by giovanni on 10/05/2014.
 * Manages subscritpions and MQTT topic rules
 */
public class MQTTTopicsManager implements ITopicsManager {

    private Vertx vertx;
    private LocalMap<String, Integer> topicsSubscribed;
    private String tenant;

    public MQTTTopicsManager(Vertx vertx, String tenant) {
        this.vertx = vertx;
        this.tenant = tenant;
        this.topicsSubscribed = this.vertx.sharedData().getLocalMap(this.tenant + "mqtt_subscribed_topics");
    }

//    public Set<String> getSubscribedTopics() {
//        return topicsSubscribed.keySet();
//    }
//    public void addSubscribedTopic(String topic) {
//        Integer retain = 0;
//        if(topicsSubscribed.keySet().contains(topic)) {
//            retain = topicsSubscribed.get(topic);
//        }
//        retain++;
//        topicsSubscribed.put(topic, retain);
//    }

//    public void removeSubscribedTopic(String topic) {
//        Integer retain = 0;
//        if(topicsSubscribed.keySet().contains(topic)) {
//            retain = topicsSubscribed.get(topic);
//        }
//        if(retain <= 0) {
//            topicsSubscribed.remove(topic);
//        }
//        else {
//            retain--;
//            topicsSubscribed.put(topic, retain);
//        }
//    }

//    public Set<String> calculateTopicsToPublish(String topicOfPublishMessage) {
//        long t1,t2,t3;
//        t1=System.currentTimeMillis();
//        Set<String> subscribedTopics = getSubscribedTopics();
//        Set<String> topicsToPublish = new LinkedHashSet<>();
//        for (String tsub : subscribedTopics) {
//            boolean ok = match(topicOfPublishMessage, tsub);
//            if(ok) {
//                topicsToPublish.add(tsub);
//            }
//        }
//        t2=System.currentTimeMillis();
//        t3=t2-t1;
//        if(t3>100) {
//            System.out.println("calculateTopicsToPublish: "+ t3 +" millis.");
//        }
//
//        return topicsToPublish;
//    }

    public boolean match(String topic, String topicFilter) {
        if(topicFilter.equals(topic)) {
            return true;
        }
        else {
            if (topicFilter.contains("+") && !topicFilter.endsWith("#")) {
                int topicSlashCount = countSlash(topic);
                int tsubSlashCount = countSlash(topicFilter);
                if (topicSlashCount == tsubSlashCount) {
                    String pattern = toPattern(topicFilter);
                    if (topic.matches(pattern)) {
                        return true;
                    }
                }
            } else if (topicFilter.contains("+") || topicFilter.endsWith("#")) {
                int topicSlashCount = countSlash(topic);
                int tsubSlashCount = countSlash(topicFilter);
                if (topicSlashCount >= tsubSlashCount) {
                    String pattern = toPattern(topicFilter);
                    if (topic.matches(pattern)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private String toPattern(String subscribedTopic) {
        String pattern = subscribedTopic;
        pattern = pattern.replaceAll("#", ".*");
        pattern = pattern.replaceAll("\\+", "[^/]*");
        return pattern;
    }

    private int countSlash(String s) {
        int count = s.replaceAll("[^/]", "").length();
        return count;
    }

//    public String toVertxTopic(String mqttTopic) {
//        String s = tenant + mqttTopic;
//        return s;
//    }
}
