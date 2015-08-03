package io.github.giovibal.mqtt.persistence;

import io.github.giovibal.mqtt.ITopicsManager;
import io.github.giovibal.mqtt.MQTTTopicsManagerOptimized;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by giova_000 on 04/06/2015.
 */
public class StoreVerticle extends AbstractVerticle {

    public static final String ADDRESS = StoreVerticle.class.getName()+"_IN";

    private Map<String, byte[]> db;
    private ITopicsManager topicsManager;

    @Override
    public void start() throws Exception {
        this.db = new LinkedHashMap<>();
        this.topicsManager = new MQTTTopicsManagerOptimized();

        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS);
        consumer.handler(message -> {
            JsonObject request = message.body();
            MultiMap headers = message.headers();
            if (headers == null || !headers.contains("command")) {
                message.reply(new JsonObject().put("error", "Invalid message: missing 'command' header"));
            }
            JsonObject response = new JsonObject();
            String command = headers.get("command");
            switch (command) {
                case "saveRetainMessage":
                    response = saveRetainMessage(request);
                    break;
                case "getRetainedMessagesByTopicFilter":
                    response = getRetainedMessagesByTopicFilter(request);
                    break;
                case "deleteRetainMessage":
                    response = deleteRetainMessage(request);
                    break;
                default:
                    response = doDefault(request);
                    break;
            }
//            System.out.println("instance => "+ this + "db.size => "+ db.size());
            message.reply(response);
        });

    }

    private JsonObject saveRetainMessage(JsonObject request) {
        String topic = request.getString("topic");
        byte[] message = request.getBinary("message");
        String tenant = request.getString("tenant");
        if(tenant!=null)
            topic = tenant + topic;
        db.put(topic, message);

        JsonObject response = new JsonObject();
        response.put("topic", topic).put("message", message);
        return response;
    }

    private JsonObject getRetainedMessagesByTopicFilter(JsonObject request) {
        String topicFilter = request.getString("topicFilter");
        String tenant = request.getString("tenant");
        if(tenant!=null) {
            topicFilter = tenant + topicFilter;
        }
//        else {
//            if(topicFilter.startsWith("/"))
//                topicFilter = "+" + topicFilter;
//            else
//                topicFilter = "+/" + topicFilter;
//        }

        List<JsonObject> list = new ArrayList<>();
        for(String topic : db.keySet()) {
            boolean topicMatch = topicsManager.match(topic, topicFilter);
            if(topicMatch) {
                byte[] message = db.get(topic);
                JsonObject item = new JsonObject().put("topic", topic).put("message", message);
                list.add(item);
            }
        }

        JsonObject response = new JsonObject();
        response.put("results", new JsonArray(list));
        return response;
    }

    private JsonObject deleteRetainMessage(JsonObject request) {
        String topic = request.getString("topic");
        String tenant = request.getString("tenant");
        if(tenant!=null)
            topic = tenant + topic;
        db.remove(topic);

        JsonObject response = new JsonObject();
        response.put("success", true);
        return response;
    }

    private JsonObject doDefault(JsonObject request) {
        JsonObject response = new JsonObject();
        return response;
    }
}
