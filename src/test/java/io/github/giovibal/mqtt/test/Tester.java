package io.github.giovibal.mqtt.test;

import io.github.giovibal.mqtt.SslUtil;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.ssl.SSLSocketFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by giovanni on 08/04/2014.
 */
public class Tester {
//    static final String serverURL = "tcp://iot.eimware.it:1883";
//    static final String serverURL = "tcp://192.168.231.52:1883";
//    static final String serverURL = "tcp://192.168.231.2:1883";

//    static final String serverURL = "tcp://127.0.0.1:1883";
    static final String serverURL = "ssl://127.0.0.1:1883";

    static boolean logEnabled=true;

    public static void main(String[] args) throws Exception {

//        test1(10);
        test2(3, 10, 0);
//        test2(2, 10000, 2);// 2 client che pubblicano 10000 messaggi ciascuno con qos:2
//        test2(10, 60000, 2);// 10 client che pubblicano 60000 messaggi ciascuno con qos:2 (235177 millis.)
//        test2(10, 60000, 0);// 10 client che pubblicano 60000 messaggi ciascuno con qos:0 (127789 millis. arrivati in media 248355 messaggi)
//        test2(10, 10000, 0);// 10 client che pubblicano 60000 messaggi ciascuno con qos:0 ( 8839 millis. arrivati in media 37000 messaggi)
//                                                                           Con Hive 2.0.2 (21535 millis. arrivati in media 93000 messaggi)
//        test2(10, 10000, 2);// 10 client che pubblicano 60000 messaggi ciascuno con qos:2 (36162 millis.)
//                                                                           Con Hive 2.0.2 (45347 millis. ma arrivati 97446 messaggi per tutti i client - NullPointer sul server)
//        test2(10, 10000, 1);// 10 client che pubblicano 60000 messaggi ciascuno con qos:1 (22609 millis.)
//                                                                           Con Hive 2.0.2 (32536 millis. ma arrivati 97435 messaggi per 9 client e 97433 per il primo)
//        test3(100);
//        test4(10, 2);
//        test4(100, 20);
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

        c.stats();

        t2=System.currentTimeMillis();
        t3=t2-t1;
        log("Time elapsed: " + t3 + " millis.");
    }


    public static void test2(int numClients, int numMessagesToPublishPerClient, int qos) throws Exception {
        String topic = "test/untopic";

        long t1,t2,t3;
        t1=System.currentTimeMillis();

        Tester cSubs = new Tester(numClients, "SUBS");
        cSubs.connect();
        cSubs.subscribe(topic);

        Tester cPubs = new Tester(numClients, "PUBS");
        cPubs.connect();

        cPubs.publish(numMessagesToPublishPerClient, topic, qos, false);
        cPubs.disconnect();

        cSubs.unsubcribe(topic);
        cSubs.disconnect();

        cPubs.publishStats();
        cSubs.subscribeStats();

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

        c.stats();

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

        c.stats();

        t2=System.currentTimeMillis();
        t3=t2-t1;
        log("Time elapsed: " + t3 + " millis.");
    }




    private List<IMqttClient> clients = new ArrayList<>();
    private List<MQTTClientHandler> clientHandlers = new ArrayList<>();

    public Tester(int numClients, String clientIDPrefix) throws MqttException {
        for(int i=1; i<=numClients; i++) {
            String clientID = clientIDPrefix+"_" + i;

            MqttClient client = new MqttClient(serverURL, clientID, new MemoryPersistence());
            MQTTClientHandler h = new MQTTClientHandler(clientID);
            client.setCallback(h);

            clients.add(client);
            clientHandlers.add(h);
        }
    }
    public void connect() throws MqttException {
        log("connect ...");
        for(IMqttClient client : clients) {
            MqttConnectOptions o = new MqttConnectOptions();
            if(this.serverURL.startsWith("ssl")) {
                try {
                        SSLSocketFactory sslSocketFactory = SslUtil.getSocketFactory(
                                "C:\\Sviluppo\\Certificati-SSL\\CA\\rootCA.pem",
//                                "C:\\Sviluppo\\Certificati-SSL\\device1\\device1_CA1.crt",
                                "C:\\Sviluppo\\Certificati-SSL\\device1\\device1.crt",
                                "C:\\Sviluppo\\Certificati-SSL\\device1\\device1.key",
                                "");
                        o.setSocketFactory(sslSocketFactory);
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
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
    public void publish(int numMessages, String topic, int qos, boolean retained) throws Exception {
        log("publih ...");
        MqttMessage m;
        for(IMqttClient client : clients) {
            for(int i=0; i<numMessages; i++) {
                String msg = "msg "+i+" qos="+qos+" retained="+ retained;
                m = new MqttMessage();
                m.setQos(qos);
                m.setRetained(retained);
                m.setPayload(msg.getBytes("UTF-8"));
                log(client.getClientId() + " publish >> sending qos=" + qos + " retained=" + retained);
                client.publish(topic, m);
            }
        }
    }

    public void stats() {
        log("-------------------------------S-T-A-T-S-------------------------------------------");
        for(MQTTClientHandler h : clientHandlers) {
            log("Client: " + h.clientID + " messaggi arrivati: " + h.messaggiArrivati + " messaggi spediti: " + h.messaggiSpediti);
        }
        log("-------------------------------S-T-A-T-S-------------------------------------------");
    }
    public void publishStats() {
        log("-------------------------------S-T-A-T-S-------------------------------------------");
        for(MQTTClientHandler h : clientHandlers) {
            log("Client: " + h.clientID + " messaggi spediti: " + h.messaggiSpediti);
        }
        log("-------------------------------S-T-A-T-S-------------------------------------------");
    }
    public void subscribeStats() {
        log("-------------------------------S-T-A-T-S-------------------------------------------");
        for(MQTTClientHandler h : clientHandlers) {
            log("Client: " + h.clientID + " messaggi arrivati: " + h.messaggiArrivati);
        }
        log("-------------------------------S-T-A-T-S-------------------------------------------");
    }
    public Map<String, Integer> getMessaggiArrivatiPerClient() {
        Map<String, Integer> ret = new HashMap<>();
        for(MQTTClientHandler h : clientHandlers) {
            ret.put(h.clientID,h.messaggiArrivati);
        }
        return ret;
    }

    static class MQTTClientHandler implements MqttCallback {

        String clientID;
        int messaggiArrivati;
        int messaggiSpediti;

        MQTTClientHandler(String clientID) {
            this.clientID = clientID;
            this.messaggiArrivati = 0;
            this.messaggiSpediti = 0;
        }

        @Override
        public void connectionLost(Throwable throwable) {
            log(clientID + " connectionLost " + throwable.getMessage());
        }

        @Override
        public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
            messaggiArrivati++;
            log(clientID + " messageArrived <== " + topic + " real qos: " + mqttMessage.getQos() + " ==> " + new String(mqttMessage.getPayload(), "UTF-8") + " " + messaggiArrivati);
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            messaggiSpediti++;
            log(clientID + " deliveryComplete ==> " + iMqttDeliveryToken.getMessageId() + " " + iMqttDeliveryToken.getClient().getClientId());
        }

    }


}
