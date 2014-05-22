package it.filippetti.smartplatform.mqtt.persistence;

import it.filippetti.smartplatform.mqtt.QOSUtils;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;

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
                .putString("topic", this.topic)
                .putNumber("qos", this.qos);
        return s;
    }

    private void fromJson(JsonObject json) {
        int qos = json.getNumber("qos", 0).intValue();
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
