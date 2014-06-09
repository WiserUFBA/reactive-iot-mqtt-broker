package io.github.giovibal.mqtt.persistence;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.platform.Container;

import java.util.*;

/**
 *
 * Foreach session, we must store a list of subscriptions ...
 * Foreach subscription, we must store a list of messages with qos 1 and 2
 *
 * session --> subscriptions list : subscriotion --> messages list
 *
 * Client_1
 *   ---> topic_a
 *       ---> message 1
 *       ---> message 2
 *       ---> message 3
 * Client_2
 *   ---> topic_a
 *       ---> message 1
 *       ---> message 2
 *       ---> message 3
 *   ---> topic_b
 *       ---> message 4
 *       ---> message 5
 *
 * N.B. Messages 1,2,3 are replicated because 2 client subscribed to same topic
 * Alternatively, we can manage a retain number and avoid duplication:
 *
 * topic        | message     | retain
 * -------------+-------------+-------------
 * topic_a      | message 1   | 2
 * topic_a      | message 2   | 2
 * topic_a      | message 3   | 2
 *
 * after one "message 1" deletion
 *
 * topic        | message     | retain
 * -------------+-------------+-------------
 * topic_a      | message 1   | 1 <-----
 * topic_a      | message 2   | 2
 * topic_a      | message 3   | 2
 *
 * pseudo-code:
 *
 * on connection (clearSession=false)
 *      restore or create session
 *      get subscribed topics by clientID from session
 *      foreach topic in session:
 *          subscribe to topic
 *          retrieve all stored messages by topic
 *          foreach messages in topic:
 *              publish message to this client
 *              remove message or decrement message retain counter (when 0 -> remove message)
 *
 * on subscribe (clearSession=false)
 *      restore session from clientID
 *      if clientID exists (only if clearSession was false):
 *          append topic to session (for long persistence)
 *
 * on unsubscribe (clearSession=false)
 *      if clientID exists (only if clearSession was false):
 *          remove topic from session
 *
 * on publish
 *      if qos = 2 or 1:
 *          store topic/message
 *
 * on publish ack
 *      if qos = 2 or 1
 *          delete topic/message
 *
 *
 * Methods:
 * restore or create session
 * getSession(String clientID)
 *
 * get subscribed topics by clientID from session
 * getTopicsByClientID(String clientID)
 *
 * retrieve all stored messages by topic
 * getMessagesByTopic(String topic)
 *
 * append topic to session (for long persistence)
 * saveTopic(String topic, Strint clientID)
 *
 * remove topic from session
 * deleteTopic(String topic, String clientID)
 *
 * store topic/message
 * saveMessage(... message, String topic)
 *
 * delete topic/message
 * deleteMessage(... message, String topic)
 *
 * TODO:
 * 1. validate debug and test logic
 * 2. consolidate an spi interface
 * 3. make some implementations
 *      - ram impl
 *      - cassandra/hbase impl
 *      - verticle/module impl (with json contract throught EventBus?)
 */
public class MQTTStoreManager {
    private Vertx vertx;
//    private Container container;
    private String tenant;

    public MQTTStoreManager(Vertx vertx/*, final Container container*/, String tenant) {
        this.vertx = vertx;
//        this.container = container;
        this.tenant = tenant;
    }


    /** append topic to session (for long persistence) */
    public void saveSubscription(Subscription subscription, String clientID) {
        String s = subscription.toString();
        vertx.sharedData().getSet(tenant + clientID).add(s);
        vertx.sharedData().getSet(tenant + "persistence.clients").add(clientID);
    }

    /** get subscribed topics by clientID from session*/
    public List<Subscription> getSubscriptionsByClientID(String clientID) {
//        container.logger().info("getSubscriptionsByClientID "+ clientID);
        ArrayList<Subscription> ret = new ArrayList<>();
        Set<String> subscriptions = vertx.sharedData().getSet(tenant + clientID);
        for(String item : subscriptions) {
            Subscription s = new Subscription();
            s.fromString(item);
            ret.add(s);
        }
        return ret;
    }

    /** remove topic from session */
    public void deleteSubcription(String topic, String clientID) {
//        container.logger().info("deleteSubcription "+ topic +" "+clientID);
        Set<String> subscriptions = vertx.sharedData().getSet(tenant + clientID);
        Set<String> copyOfSubscriptions = new LinkedHashSet<>(subscriptions);
        for(String item : copyOfSubscriptions) {
            Subscription s = new Subscription();
            s.fromString(item);
            if(s.getTopic().equals(topic)) {
                subscriptions.remove(item);
            }
        }
        if(subscriptions.isEmpty()) {
            vertx.sharedData().removeSet(tenant + clientID);
            vertx.sharedData().getSet(tenant + "persistence.clients").remove(clientID);
        }
    }

    public Set<String> getClientIDs() {
        return vertx.sharedData().getSet(tenant + "persistence.clients");
    }


    private Map<String, Integer> seq() {
        Map<String, Integer> seq = vertx.sharedData().getMap(tenant + "sequence");
        return seq;
    }
    private Integer currentID(String k) {
        Integer currentID=0;
        Map<String, Integer> seq = seq();
        if(!seq.containsKey(k)) {
            seq.put(k, 0);
        }
        currentID = seq.get(k);
        return currentID;
    }
    private void incrementID(String k) {
        Integer currentID = currentID(k);
        Integer nextID = currentID+1;
        seq().put(k, nextID);
    }
    private void decrementID(String k) {
        Integer currentID=0;
        Map<String, Integer> seq = seq();
        if(seq.containsKey(k)) {
            currentID = seq.get(k);
            if (currentID > 0) {
                seq.put(k, currentID - 1);
            }
        }
    }


    /** store topic/message */
    public void pushMessage(byte[] message, String topic) {
        Set<String> clients = getClientIDs();
        for(String clientID : clients) {
            List<Subscription> subscriptions = getSubscriptionsByClientID(clientID);
            for(Subscription s : subscriptions) {
                if(s.getTopic().equals(topic)) {
                    String key = clientID + topic;
                    incrementID(key);
                    String k = "" + currentID(key);
                    vertx.sharedData().getMap(tenant + key).put(k, message);
                }
            }
        }
    }

    /** retrieve all stored messages by topic */
    public List<byte[]> getMessagesByTopic(String topic, String clientID) {
        String key  = clientID+topic;
        Map<String, byte[]> set = vertx.sharedData().getMap(tenant + key);
        ArrayList<byte[]> ret = new ArrayList<>(set.values());
        return ret;
    }

    /** delete topic/message */
    public byte[] popMessage(String topic, String clientID) {
        String key  = clientID+topic;
        String k = ""+currentID(key);
        Map<String, byte[]> set = vertx.sharedData().getMap(tenant + key);
        if(set.containsKey(k)) {
            byte[] removed = set.remove(k);
            decrementID(key);
            return removed;
        }
        return null;
    }

}
