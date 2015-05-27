package io.github.giovibal.mqtt.persistence;

import io.github.giovibal.mqtt.MQTTJson;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class MQTTStoreManagerAsync {
    private Vertx vertx;
    private String tenant;

    public MQTTStoreManagerAsync(Vertx vertx, String tenant) {
        this.vertx = vertx;
        this.tenant = tenant;
    }


    /** append topic to session (for long persistence) */
    public void saveSubscription(Subscription subscription, String clientID) {
        String s = subscription.toString();
        vertx.sharedData().getLocalMap(tenant + clientID).put(s, 1);
        vertx.sharedData().getLocalMap(tenant + "persistence.clients").put(clientID, 1);
    }

    /** get subscribed topics by clientID from session*/
    public void getSubscriptionsByClientID(String clientID, Handler<List<Subscription>> handler) {
        ArrayList<Subscription> ret = new ArrayList<>();
        LocalMap<String, Object> subscriptions = vertx.sharedData().getLocalMap(tenant + clientID);
        for(String item : subscriptions.keySet()) {
            Subscription s = new Subscription();
            s.fromString(item);
            ret.add(s);
        }
        handler.handle(ret);
    }

    /** remove topic from session */
    public void deleteSubcription(String topic, String clientID) {
        LocalMap<String, Object> subscriptionsMap = vertx.sharedData().getLocalMap(tenant + clientID);
        Set<String> subscriptions = subscriptionsMap.keySet();
        Set<String> copyOfSubscriptions = new LinkedHashSet<>(subscriptions);
        for(String item : copyOfSubscriptions) {
            Subscription s = new Subscription();
            s.fromString(item);
            if(s.getTopic().equals(topic)) {
                subscriptions.remove(item);
            }
        }
        if(subscriptions.isEmpty()) {
            vertx.sharedData().getLocalMap(tenant + "persistence.clients").remove(clientID);
        }
    }

    public void getClientIDs(Handler<Set<String>> handler) {
        LocalMap<String, Object> m = vertx.sharedData().getLocalMap(tenant + "persistence.clients");
        handler.handle( m.keySet() );
    }


    private LocalMap<String, Integer> seq() {
        LocalMap<String, Integer> seq = vertx.sharedData().getLocalMap(tenant + "sequence");
        return seq;
    }
    private Integer currentID(String k) {
        Integer currentID=0;
        LocalMap<String, Integer> seq = seq();
        if(!seq.keySet().contains(k)) {
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
        LocalMap<String, Integer> seq = seq();
        if(seq.keySet().contains(k)) {
            currentID = seq.get(k);
            if (currentID > 0) {
                seq.put(k, currentID - 1);
            }
        }
    }


    /** store topic/message */
    public void pushMessage(byte[] message, String topic) {
        getClientIDs(clients -> {
            for(String clientID : clients) {
                getSubscriptionsByClientID(clientID, subscriptions -> {
                    for(Subscription s : subscriptions) {
                        if(s.getTopic().equals(topic)) {
                            String key = tenant + clientID + topic;
                            incrementID(key);
                            String k = "" + currentID(key);
                            vertx.sharedData().getLocalMap(key).put(k, message);
                        }
                    }
                });
            }
        });
    }
    public void saveMessage(byte[] message, String topic) {
        String key = topic;
        vertx.sharedData().getLocalMap(tenant).put(key, message);
    }
    public void deleteMessage(String topic) {
        String key = topic;
        LocalMap<String, byte[]> map = vertx.sharedData().getLocalMap(tenant);
        if(map.keySet().contains(key)) {
            map.remove(key);
        }
    }

    /** retrieve all stored messages by topic */
    public void getMessagesByTopic(String topic, String clientID, Handler<List<byte[]>> handler) {
        String key  = clientID+topic;
        LocalMap<String, byte[]> set = vertx.sharedData().getLocalMap(tenant + key);
        LocalMap<String, byte[]> set2 = vertx.sharedData().getLocalMap(tenant);
        ArrayList<byte[]> ret = new ArrayList<>();
        ret.addAll(set.values());
        ret.addAll(set2.values());
        handler.handle( ret );

//        JsonObject data = new JsonObject()
//                .put("tenant", tenant)
//                .put("topic", topic)
//                .put("clientID", clientID);
//
//        vertx.eventBus().send("io.github.giovibal.mqtt.persistence",
//                data,
//                new DeliveryOptions()
//                        .addHeader("action", "getMessagesByTopic")
//                        .setSendTimeout(30 * 1000),
//                new Handler<AsyncResult<Message<JsonObject>>>() {
//                    @Override
//                    public void handle(AsyncResult<Message<JsonObject>> event) {
//                        ArrayList<byte[]> ret = new ArrayList<>();
//
//                        JsonObject result = event.result().body();
//                        JsonArray array = result.getJsonArray("result");
//                        for (int i = 0; i < array.size(); i++) {
//                            byte[] item = array.getBinary(i);
//                            ret.add(item);
//                            handler.handle(ret);
//                        }
//                    }
//                }
//        );
    }

    /** get and delete topic/message */
    public void popMessage(String topic, String clientID, Handler<byte[]> handler) {
        String key  = clientID+topic;
        String k = ""+currentID(key);
        LocalMap<String, byte[]> set = vertx.sharedData().getLocalMap(tenant + key);
        if(set.keySet().contains(k)) {
            byte[] removed = set.remove(k);
            decrementID(key);
            handler.handle( removed );
        }
        handler.handle( null );
    }


    public void storeWillMessage(String willMsg, byte willQos, String willTopic) {
        MQTTJson mqttJson = new MQTTJson();
        JsonObject wm = mqttJson.serializeWillMessage(willMsg, willQos, willTopic);
        vertx.sharedData().getLocalMap("will_messages").put(willTopic, wm);
    }




    public void addClientID(String clientID) {
        vertx.sharedData().getLocalMap("clientIDs").put(clientID, 1);
    }
    public void clientIDExists(String clientID, Handler<Boolean> handler) {
        LocalMap<String, Object> m = vertx.sharedData().getLocalMap("clientIDs");
        if(m!=null)
            handler.handle(m.keySet().contains(clientID));
        handler.handle( false );
    }
    public void removeClientID(String clientID) {
        vertx.sharedData().getLocalMap("clientIDs").remove(clientID);
    }
}
