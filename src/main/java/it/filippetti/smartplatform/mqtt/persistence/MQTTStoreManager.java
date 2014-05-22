package it.filippetti.smartplatform.mqtt.persistence;

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
 *      restore session from clientID (private String clientID --> variable in MQTTSocket instance populated during connect)
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
 */
public class MQTTStoreManager {
    private Vertx vertx;
    private Container container;

    public MQTTStoreManager(Vertx vertx, Container container) {
        this.vertx = vertx;
        this.container = container;
    }


    /** append topic to session (for long persistence) */
    public void saveSubscription(Subscription subscription, String clientID) {
        String s = subscription.toString();
        vertx.sharedData().getSet(clientID).add(s);
    }

    /** get subscribed topics by clientID from session*/
    public List<Subscription> getSubscriptionsByClientID(String clientID) {
        container.logger().info("getSubscriptionsByClientID");
        ArrayList<Subscription> ret = new ArrayList<>();
        Set<String> subscriptions = vertx.sharedData().getSet(clientID);
        for(String item : subscriptions) {
            Subscription s = new Subscription();
            s.fromString(item);
            ret.add(s);
        }
        return ret;
    }

    /** remove topic from session */
    public void deleteSubcription(String topic, String clientID) {
        container.logger().info("deleteSubcription");
        Set<String> subscriptions = vertx.sharedData().getSet(clientID);
        Set<String> copyOfSubscriptions = new LinkedHashSet<>(subscriptions);
        for(String item : copyOfSubscriptions) {
            Subscription s = new Subscription();
            s.fromString(item);
            if(s.getTopic().equals(topic)) {
                subscriptions.remove(item);
            }
        }
        if(subscriptions.isEmpty()) {
            vertx.sharedData().removeSet(clientID);
        }
    }




    /** store topic/message */
    public void saveMessage(String messageKey, byte[] message, String topic) {
        container.logger().info("saveMessage");
        vertx.sharedData().getMap(topic).put(messageKey, message);
    }

    /** retrieve all stored messages by topic */
    public List<byte[]> getMessagesByTopic(String topic) {
        container.logger().info("getMessagesByTopic");
        Map<String, byte[]> set = vertx.sharedData().getMap(topic);
        ArrayList<byte[]> ret = new ArrayList<>(set.values());
        return ret;
    }

    /** delete topic/message */
    public void deleteMessage(String messageKey, byte[] message, String topic) {
        container.logger().info("deleteMessage");
        Map<String, byte[]> set = vertx.sharedData().getMap(topic);
        set.remove(messageKey);

        if(set.isEmpty()) {
            vertx.sharedData().removeMap(topic);
        }
    }
}
