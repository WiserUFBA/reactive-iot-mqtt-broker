package io.github.giovibal.mqtt.persistence;

import io.github.giovibal.mqtt.ITopicsManager;
import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by giova_000 on 03/06/2015.
 */
public class StoreManager {

    private Vertx vertx;
    private String tenant;
    private ITopicsManager topicsManager;
    private MQTTEncoder encoder;
    private MQTTDecoder decoder;

    public StoreManager(Vertx vertx, String tenant, ITopicsManager topicsManager) {
        this.vertx = vertx;
        this.tenant = tenant;
        this.topicsManager = topicsManager;
        this.encoder = new MQTTEncoder();
        this.decoder = new MQTTDecoder();
    }


    public void saveRetainMessage(PublishMessage pm, Handler<Boolean> onComplete) {
        try {
            String topic = pm.getTopicName();
            Buffer pmBytes = encoder.enc(pm);

//            LocalMap<String, Buffer> retained = vertx.sharedData().getLocalMap("retained");
//            retained.put(topic, pmBytes);
            JsonObject request = new JsonObject()
                    .put("topic", topic)
                    .put("message", pmBytes.getBytes());
//            vertx.eventBus().send(
//                    StoreVerticle.ADDRESS,
//                    request,
//                    new DeliveryOptions().addHeader("command", "saveRetainMessage"),
//                    (AsyncResult<Message<JsonObject>> res) -> {
//                        if (res.succeeded()) {
//                            onComplete.handle(Boolean.TRUE);
//                        } else {
//                            onComplete.handle(Boolean.FALSE);
//                        }
//                    });
            vertx.eventBus().publish(
                    StoreVerticle.ADDRESS,
                    request,
                    new DeliveryOptions().addHeader("command", "saveRetainMessage"));

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void getRetainedMessagesByTopicFilter(String topicFilter, Handler<List<PublishMessage>> handler) {
        List<PublishMessage> list = new ArrayList<>();
//        LocalMap<String, Buffer> retained = vertx.sharedData().getLocalMap("retained");

        JsonObject request = new JsonObject()
                .put("topicFilter", topicFilter);
        vertx.eventBus().send(
                StoreVerticle.ADDRESS,
                request,
                new DeliveryOptions().addHeader("command", "getRetainedMessagesByTopicFilter"),
                (AsyncResult<Message<JsonObject>> res) -> {
                    if (res.succeeded()) {
                        Message<JsonObject> msg = res.result();
                        JsonObject response = msg.body();
                        JsonArray results = response.getJsonArray("results");
                        List<JsonObject> retained = (List<JsonObject>) results.getList();
                        for (JsonObject item : retained) {
                            try {
                                String topic = item.getString("topic");
                                byte[] message = item.getBinary("message");
                                Buffer pmBytes = Buffer.buffer(message);
                                PublishMessage pm = (PublishMessage) decoder.dec(pmBytes);
//                                boolean topicMatch = topicsManager.match(pm.getTopicName(), topicFilter);
//                                if (topicMatch) {
                                    list.add(pm);
//                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                            }
                        }
                        handler.handle(list);
                    }
                });

    }
}
