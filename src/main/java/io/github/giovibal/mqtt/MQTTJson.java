package io.github.giovibal.mqtt;

import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.vertx.java.core.json.JsonObject;

import java.nio.ByteBuffer;

/**
 * Created by giovanni on 14/04/14.
 * JSON Utility class
 */
public class MQTTJson {

    public boolean isDeserializable(JsonObject json) {
        boolean ret = (
               json.getFieldNames().contains("topicName")
//          && json.getFieldNames().contains("qos")
            && json.getFieldNames().contains("payload")
        );
        return ret;
    }

    public JsonObject serializePublishMessage(PublishMessage publishMessage) {
        JsonObject ret = new JsonObject();

        ret.putString("topicName", publishMessage.getTopicName());
        ret.putString("qos", publishMessage.getQos().name());
        ret.putBinary("payload", publishMessage.getPayload().array());
        if(publishMessage.getQos() == AbstractMessage.QOSType.LEAST_ONE || publishMessage.getQos() == AbstractMessage.QOSType.EXACTLY_ONCE) {
            ret.putNumber("messageID", publishMessage.getMessageID());
        }
        return ret;
    }
    public PublishMessage deserializePublishMessage(JsonObject json) {
        PublishMessage ret = new PublishMessage();
        ret.setTopicName(json.getString("topicName"));
        AbstractMessage.QOSType qos = AbstractMessage.QOSType.valueOf(json.getString("qos"));
        ret.setQos(qos);
        byte[] payload = json.getBinary("payload");
        ret.setPayload(ByteBuffer.wrap(payload));
        if(qos == AbstractMessage.QOSType.LEAST_ONE || qos == AbstractMessage.QOSType.EXACTLY_ONCE) {
            ret.setMessageID(json.getNumber("messageID").intValue());
        }
        return ret;
    }

    public JsonObject serializeWillMessage(String willMsg, byte willQos, String willTopic) {
        JsonObject wm = new JsonObject()
                .putString("topicName",willTopic)
                .putNumber("qos", new Integer(willQos))
                .putString("message",willMsg);
        return wm;
    }

}
