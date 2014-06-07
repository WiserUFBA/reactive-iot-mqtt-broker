package io.github.giovibal.mqtt.test;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Created by giovanni on 24/05/2014.
 */
public class DefaultMqttHandler implements MqttCallback {
    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("connectionLost");
        throwable.printStackTrace();
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        System.out.println(topic +" ==> "+ new String(mqttMessage.getPayload(), "UTF-8"));
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("deliveryComplete ==> "+ iMqttDeliveryToken.getMessageId());
    }
}
