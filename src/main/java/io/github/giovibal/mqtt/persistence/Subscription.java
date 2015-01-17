package io.github.giovibal.mqtt.persistence;

import io.github.giovibal.mqtt.QOSUtils;
import io.vertx.core.json.JsonObject;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

/**
 * Created by giovanni on 21/05/2014.
 */
public class Subscription {
    private String topic;
    private int qos;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    private JsonObject toJson() {
        JsonObject s = new JsonObject()
                .put("topic", this.topic)
                .put("qos", this.qos);
        return s;
    }

    private void fromJson(JsonObject json) {
        int qos = json.getInteger("qos", 0);
        AbstractMessage.QOSType qosType = new QOSUtils().toQos(qos);
        this.qos = new QOSUtils().toByte(qosType);
        this.topic = json.getString("topic");
    }

    public void fromString(String s) {
        JsonObject json = new JsonObject(s);
        fromJson(json);
    }
    public String toString() {
        String s = toJson().encode();
        return s;
    }

}
