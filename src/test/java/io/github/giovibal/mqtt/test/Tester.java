package io.github.giovibal.mqtt.test;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.*;

/**
 * Created by giovanni on 08/04/2014.
 */
public class Tester {
    static final String serverURL = "tcp://localhost:1883";
//    static final String serverURL = "tcp://192.168.231.2:1883";

    static boolean logEnabled=true;

    public static void main(String[] args) throws Exception {

//        test1(10);
//        test2(10);
//        test3(10);
//        test4(10, 2);
        test4(100, 20);
//        logEnabled=false;
//        test4(1000, 200);
    }

    private static void log(String msg) {
        if(logEnabled) {
            System.out.println(msg);
        }
    }

    public static void test1(int numClients) throws Exception {
        String topic = "test/untopic";

        long t1,t2,t3;
        t1=System.currentTimeMillis();

        Tester c = new Tester(numClients, "Paho");
        c.connect();
        c.subscribe(topic);
        c.publish(topic);

//        log("Wait 10 seconds ...");
//        Thread.sleep(10000);

        c.unsubcribe(topic);
        c.disconnect();

        stats();

        t2=System.currentTimeMillis();
        t3=t2-t1;
        log("Time elapsed: " + t3 + " millis.");
    }


    public static void test2(int numClients) throws Exception {
        String topic = "test/untopic";

        long t1,t2,t3;
        t1=System.currentTimeMillis();

        Tester csubs = new Tester(numClients, "SUBS");
        csubs.connect();
        csubs.subscribe(topic);

        Tester c = new Tester(numClients, "PUBS");
        c.connect();
        c.publish(topic);
        c.disconnect();

        csubs.unsubcribe(topic);
        csubs.disconnect();

        stats();

        t2=System.currentTimeMillis();
        t3=t2-t1;
        log("Time elapsed: " + t3 + " millis.");
    }

    public static void test3(int numClients) throws Exception {
        String topicPub = "test/publish";
        String topicSub = "test/+";

        long t1,t2,t3;
        t1=System.currentTimeMillis();

        Tester c = new Tester(numClients, "Paho");
        c.connect();
        c.subscribe(topicSub);
        c.publish(topicPub);
        c.unsubcribe(topicSub);
        c.disconnect();

        stats();

        t2=System.currentTimeMillis();
        t3=t2-t1;
        log("Time elapsed: " + t3 + " millis.");
    }

    public static void test4(int numClients, int numTopics) throws Exception {
        String topicPrefix = "test/topic";

        long t1,t2,t3;
        t1=System.currentTimeMillis();

        Tester c = new Tester(numClients, "Paho");
        c.connect();
        for(int i=0; i<numTopics; i++) {
            String topic = topicPrefix + "/" + i;
            c.subscribe(topic);
        }
        for(int i=0; i<numTopics; i++) {
            String topic = topicPrefix + "/" + i;
            c.publish(topic);
        }
        for(int i=0; i<numTopics; i++) {
            String topic = topicPrefix + "/" + i;
            c.unsubcribe(topic);
        }
        c.disconnect();

        stats();

        t2=System.currentTimeMillis();
        t3=t2-t1;
        log("Time elapsed: " + t3 + " millis.");
    }




    private List<IMqttClient> clients;
    public Tester(int numClients, String clientIDPrefix) throws MqttException {
        clients = new ArrayList<>();
        for(int i=1; i<=numClients; i++) {
            String clientID = clientIDPrefix+"_" + i;

            MqttClient client = new MqttClient(serverURL, clientID, new MemoryPersistence());
            client.setCallback(new MQTTClientHandler(clientID));
            clients.add(client);
        }

    }
    public void connect() throws MqttException {
        log("connet ...");
        for(IMqttClient client : clients) {
            MqttConnectOptions o = new MqttConnectOptions();
            o.setCleanSession(true);
            client.connect(o);
        }
    }

    public void disconnect() throws MqttException {
        log("disconnet ...");
        for(IMqttClient client : clients) {
            client.disconnect();
        }
    }

    public void subscribe(String topic) throws MqttException {
        log("subscribe topic: " + topic + " ...");
        for (IMqttClient client : clients) {
            client.subscribe(topic, 2);
        }
    }
    public void unsubcribe(String topic) throws MqttException {
        log("unsubscribe topic: " + topic + " ...");
        for (IMqttClient client : clients) {
            client.unsubscribe(topic);
        }
    }

    private static int sleep = 5000;
    public void publish(String topic) throws Exception {
        log("publih ...");
        MqttMessage m;
        for(IMqttClient client : clients) {

            m = new MqttMessage();
            m.setQos(2);
            m.setRetained(true);
            m.setPayload("prova qos=2 retained=true".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=2 retained=true");
            client.publish(topic, m);

            m = new MqttMessage();
            m.setQos(1);
            m.setRetained(true);
            m.setPayload("prova qos=1 retained=true".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=1 retained=true");
            client.publish(topic, m);



            m = new MqttMessage();
            m.setQos(0);
            m.setRetained(true);
            m.setPayload("prova qos=0 retained=true".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=0 retained=true");
            client.publish(topic, m);


            m = new MqttMessage();
            m.setQos(2);
            m.setRetained(false);
            m.setPayload("prova qos=2 retained=false".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=2 retained=false");
            client.publish(topic, m);


            m = new MqttMessage();
            m.setQos(1);
            m.setRetained(false);
            m.setPayload("prova qos=1 retained=false".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=1 retained=false");
            client.publish(topic, m);


            m = new MqttMessage();
            m.setQos(0);
            m.setRetained(false);
            m.setPayload("prova qos=0 retained=false".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=0 retained=false");
            client.publish(topic, m);


        }
    }

    public static void stats() {
        Set<String> keys = messaggiArrivatiByClient.keySet();
        for(String clientID : keys) {
            Integer count = messaggiArrivatiByClient.get(clientID);
            log("Client: " + clientID + " messaggi arrivati: " + count);
        }
    }

    public static Map<String, Integer> messaggiArrivatiByClient = new HashMap<>();
    static class MQTTClientHandler implements MqttCallback {

        String clientID;
        int messaggiArrivati;
        MQTTClientHandler(String clientID) {
            this.clientID = clientID;
            this.messaggiArrivati = 0;
        }

        @Override
        public void connectionLost(Throwable throwable) {
            log(clientID + " connectionLost " + throwable.getMessage());
        }

        @Override
        public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
            messaggiArrivati++;
            log(clientID + " messageArrived << " + topic + " real qos: " + mqttMessage.getQos() + " ==> " + new String(mqttMessage.getPayload(), "UTF-8") + " " + messaggiArrivati);
            messaggiArrivatiByClient.put(clientID, messaggiArrivati);
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            log(clientID + " deliveryComplete ==> " + iMqttDeliveryToken.getMessageId() + " " + iMqttDeliveryToken.getClient().getClientId());
        }
    }
}
