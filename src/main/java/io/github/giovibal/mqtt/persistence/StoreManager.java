package io.github.giovibal.mqtt.persistence;

import io.github.giovibal.mqtt.ITopicsManager;
import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;

import java.util.*;

/**
 * Created by giova_000 on 03/06/2015.
 */
public class StoreManager {

    private Vertx vertx;
    private String tenant;
    private ITopicsManager topicsManager;
    private MQTTEncoder encoder;
    private MQTTDecoder decoder;

    private static Map<String, Buffer> retained = Collections.synchronizedMap( new LinkedHashMap<>() );

    public StoreManager(Vertx vertx, String tenant, ITopicsManager topicsManager) {
        this.vertx = vertx;
        this.tenant = tenant;
        this.topicsManager = topicsManager;
        this.encoder = new MQTTEncoder();
        this.decoder = new MQTTDecoder();
        // TODO: use shared clustered map
//        this.retained = new LinkedHashMap<>();
    }

    public void saveRetainMessage(PublishMessage pm, Handler<Boolean> onComplete) {
        try {
            String topic = pm.getTopicName();
            Buffer pmBytes = encoder.enc(pm);
            retained.put(topic, pmBytes);
            onComplete.handle(Boolean.TRUE);
//            vertx.sharedData().getClusterWideMap("retained", (AsyncResult<AsyncMap<String, Buffer>> event) -> {
//                if(event.succeeded()) {
//                    event.result().put(topic, pmBytes, completionHandler -> {
//                        System.out.println("saved");
//                        onComplete.handle(Boolean.TRUE);
//                    });
//                } else {
//                    onComplete.handle(Boolean.FALSE);
//                }
//            });
        } catch(Throwable e) {
            e.printStackTrace();
        }
    }
    public void getRetainedMessagesByTopicFilter(String topicFilter, Handler<List<PublishMessage>> handler) {
        List<PublishMessage> list = new ArrayList<>();
        for(Buffer pmBytes : retained.values()) {
            try {
                PublishMessage pm = (PublishMessage)decoder.dec(pmBytes);
                boolean topicMatch = topicsManager.match(pm.getTopicName(), topicFilter);
                if(topicMatch) {
                    list.add(pm);
                }
            } catch(Throwable e) {
                e.printStackTrace();
            }
        }
        handler.handle( list );
    }


}
