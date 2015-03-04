package io.github.giovibal.mqtt;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by giovanni on 10/05/2014. Manages subscritpions and MQTT topic rules
 */
public class MQTTTopicsManager2 implements ITopicsManager {
    public static class SubscriptionTopic {
        private String topic;
        private String vertxTopic;
        private String tenant;
        private Pattern regexPattern;

        public SubscriptionTopic(String topic) {
            this.topic = topic;
        }

        public String getTenant() {
            return tenant;
        }

        public void setTenant(String tenant) {
            this.tenant = tenant;
        }

        public Pattern getRegexPattern() {
            return regexPattern;
        }

        public void setRegexPattern(Pattern regexPattern) {
            this.regexPattern = regexPattern;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getVertxTopic() {
            return vertxTopic;
        }

        public void setVertxTopic(String vertxTopic) {
            this.vertxTopic = vertxTopic;
        }
    }

    private Vertx vertx;
    private LocalMap<String, Integer> topicsSubscribed;
    private Map<String, SubscriptionTopic> topicsSubscribedMap = new LinkedHashMap<String, SubscriptionTopic>();
    private String tenant;

    public MQTTTopicsManager2(Vertx vertx, String tenant) {
        this.vertx = vertx;
        this.tenant = tenant;
        this.topicsSubscribed = this.vertx.sharedData().getLocalMap(this.tenant + ".mqtt_subscribed_topics");

		/*
		 * TODO: capire meglio come reingegnerizzare l'uso delle mappe
		 * cluster-wide
		 */
        // this.vertx.sharedData().getClusterWideMap(this.tenant +
        // "mqtt_subscribed_topics", new AsyncResultHandler<AsyncMap<String,
        // Integer>>() {
        // @Override
        // public void handle(AsyncResult<AsyncMap<String, Integer>>
        // asyncMapAsyncResult) {
        // topicsSubscribed = asyncMapAsyncResult.result();
        // }
        // });
    }

    public void addSubscribedTopic(String topic) {
        Integer subscriptionCounter = topicsSubscribed.get(topic);
        subscriptionCounter = subscriptionCounter != null ? subscriptionCounter++ : 1;
        topicsSubscribed.put(topic, subscriptionCounter);

        // /* synchronize */
        // for(String tt : topicsSubscribed.keySet()) {
        // createSubscriptionTopic(tt);
        // }

		/* synchronize */
        Set<String> ks = topicsSubscribed.keySet();
        Iterator<String> ii = topicsSubscribedMap.keySet().iterator();
        while (ii.hasNext()) {
            String kk = ii.next();
            if (!ks.contains(kk)) {
                topicsSubscribedMap.remove(kk);
            }
        }
    }

    @Deprecated
    public Set<String> calculateTopicsToPublish(String topicOfPublishMessage) {
        long t1, t2, t3;
        t1 = System.currentTimeMillis();
        String topic = topicOfPublishMessage;
        Set<String> subscribedTopics = getSubscribedTopics();
        Set<String> topicsToPublish = new LinkedHashSet<>();
        for (String tsub : subscribedTopics) {
            boolean ok = match(topic, tsub);
            if (ok) {
                topicsToPublish.add(tsub);
            }
        }
        t2 = System.currentTimeMillis();
        t3 = t2 - t1;
        if (t3 > 100) {
            System.out.println("calculateTopicsToPublish: " + t3 + " millis.");
        }
        return topicsToPublish;
    }

    private int countSlash(String s) {
        int count = s.replaceAll("[^/]", "").length();
        return count;
    }

    public Set<String> getSubscribedTopics() {
        return topicsSubscribed.keySet();
    }

    // public Collection<SubscriptionTopic> getSubscriptionTopics() {
    // return topicsSubscribed.values();
    // }

    @Deprecated
    public boolean match(String topic, String topicFilter) {
        String tsub = topicFilter;
        if (tsub.equals(topic)) {
            return true;
        }
        else {
            if (tsub.contains("+") && !tsub.endsWith("#")) {
                String pattern = toPattern(tsub);
                int topicSlashCount = countSlash(topic);
                int tsubSlashCount = countSlash(tsub);
                if (topicSlashCount == tsubSlashCount) {
                    if (topic.matches(pattern)) {
                        return true;
                    }
                }
            }
            else if (tsub.contains("+") || tsub.endsWith("#")) {
                String pattern = toPattern(tsub);
                int topicSlashCount = countSlash(topic);
                int tsubSlashCount = countSlash(tsub);
                if (topicSlashCount >= tsubSlashCount) {
                    if (topic.matches(pattern)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public SubscriptionTopic createSubscriptionTopic(String topic) {
        SubscriptionTopic st = topicsSubscribedMap.get(topic);
        if (st == null) {
            st = new SubscriptionTopic(topic);
            st.setVertxTopic(toVertxTopic(topic));
            st.setRegexPattern(toRegexPattern(topic));

            // System.out.println("ST = " + st.getTopic() + " -> " +
            // st.getVertxTopic());

            topicsSubscribedMap.put(topic, st);
        }
        return st;
    }

    public void removeSubscribedTopic(String topic) {
        Integer retain = topicsSubscribed.get(topic);

        if (retain != null) {
            retain--;
            if (retain > 0) {
                topicsSubscribed.put(topic, retain);
            }
            else {
                topicsSubscribed.remove(topic);
//				topicsSubscribedMap.remove(topic);
            }

			/* synchronize */
            Set<String> ks = topicsSubscribed.keySet();
            Iterator<String> ii = topicsSubscribedMap.keySet().iterator();
            while (ii.hasNext()) {
                String kk = ii.next();
                if (!ks.contains(kk)) {
                    topicsSubscribedMap.remove(kk);
                }
            }
            // // System.out.println("SUB -= " + topic + " #" + retain);
            // // System.out.println("SUB MAP = " + topicsSubscribed.keySet());
            // // System.out.println("SUB REG = " +
            // // topicsSubscribedRegex.keySet());
        }
    }

    private String toPattern(String subscribedTopic) {
        String pattern = subscribedTopic;
        pattern = pattern.replaceAll("\\+", ".+?");
        pattern = pattern.replaceAll("/#", "/.+");
        return pattern;
    }

    private Pattern toRegexPattern(String subscribedTopic) {
        String regexPattern = subscribedTopic;
        regexPattern = regexPattern.replaceAll("\\#", ".*");
        regexPattern = regexPattern.replaceAll("\\+", "[^/]*");

        Pattern pattern = Pattern.compile(regexPattern);

        return pattern;
    }

    public String toVertxTopic(String mqttTopic) {
        // String s = tenant +"/"+ mqttTopic;
        // s = s.replaceAll("/+","/"); // remove multiple slashes
        String s = mqttTopic;
        if (tenant != null && !tenant.isEmpty()) {
            s = tenant + "/" + mqttTopic;
            s = s.replaceAll("/+", "/"); // remove multiple slashes
        }
        return s;
    }

}

