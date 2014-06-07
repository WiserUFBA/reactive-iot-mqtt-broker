package io.github.giovibal.mqtt.test;

import org.eclipse.paho.client.mqttv3.*;

/**
 * Created by giovanni on 19/05/2014.
 */
public class HAClient {

    public static void main(String[] args) throws Exception {
        long t1,t2,t3;
        t1 = System.currentTimeMillis();
        boolean allTestsOk = true;
        String uri;
        uri = "tcp://localhost:1883";
//        uri = "tcp://iot.eclipse.org:1883";
//        uri = "tcp://localhost:61613";
        HAClient hac = new HAClient(uri);
        hac.test(2);
        allTestsOk &= hac.allTestsPass;
        hac.test(2);
        allTestsOk &= hac.allTestsPass;
        hac.test(2);
        allTestsOk &= hac.allTestsPass;
        hac.test(1);
        allTestsOk &= hac.allTestsPass;
        hac.test(1);
        allTestsOk &= hac.allTestsPass;
        hac.test(1);
        allTestsOk &= hac.allTestsPass;

        System.out.println();
        System.out.println("------------------------------------------------");
        if(allTestsOk) {
            System.out.println("All tests pass correctly!");
            System.out.println("This broker support QoS 1 and 2 with cleanSession=false");
        } else {
            System.out.println("Some test failed...");
        }

        t2 = System.currentTimeMillis();
        t3 = t2-t1;
        System.out.println("Time elapsed: "+ t3 + " millis.");
        System.out.println("------------------------------------------------");
        System.out.println();
    }

    private MqttConnectOptions options;
    private MqttClient client1;
    private MqttClient client2;
    private MqttClient publisher;
    private boolean allTestsPass = false;

    public HAClient(String uri) throws MqttException {

        options = new MqttConnectOptions();
        options.setCleanSession(false);
        options.setConnectionTimeout(1000);
        options.setKeepAliveInterval(1000);
        options.setUserName("admin");
        options.setPassword("password".toCharArray());

        client1 = new MqttClient(uri, "ha_client_1");
        client2 = new MqttClient(uri, "ha_client_2");

        publisher = new MqttClient(uri, "publisher");
        publisher.setCallback(new DefaultMqttHandler());
    }

    public void test(int qos) throws Exception {
        // staser server session (with cleanSession=false)...
        client1.connect(options);
        client1.subscribe("test/untopic");
        client1.subscribe("test/+/b");
        client1.disconnect();

        client2.connect(options);
        client2.subscribe("test/a/#");
        client2.subscribe("test/c/+");
        client2.disconnect();

//        Thread.sleep(5000);

        // publish meanwhile first client is disconnected...
        publisher.connect(options);
        publisher.publish("test/untopic", "messaggio topic => test/untopic".getBytes("UTF-8"), qos, false);
        publisher.publish("test/a/b", "messaggio topic => test/a/b".getBytes("UTF-8"), qos, false);
        publisher.publish("test/c/b", "messaggio topic => test/c/b".getBytes("UTF-8"), qos, false);
        publisher.disconnect();

//        Thread.sleep(5000);

        // reconnect... will receive message...
        HAMqttHandler mqttHandler1 = new HAMqttHandler();
        client1.setCallback(mqttHandler1);
        client1.connect(options);
        client1.subscribe("test/untopic");
        client1.subscribe("test/+/b");
        client1.disconnect();

        HAMqttHandler mqttHandler2 = new HAMqttHandler();
        client2.setCallback(mqttHandler2);
        client2.connect(options);
        client2.subscribe("test/a/#");
        client2.subscribe("test/c/+");
        client2.disconnect();

        // reconnect... second time ... no more messages to consume ...
        HAMqttHandler mqttHandler1b = new HAMqttHandler ();
        client1.setCallback(mqttHandler1b);
        client1.connect(options);
        client1.subscribe("test/untopic");
        client1.subscribe("test/+/b");
        client1.unsubscribe("test/untopic");
        client1.unsubscribe("test/+/b");
        client1.disconnect();

        HAMqttHandler mqttHandler2b = new HAMqttHandler ();
        client2.setCallback(mqttHandler2b);
        client2.connect(options);
        client2.subscribe("test/a/#");
        client2.subscribe("test/c/+");
//        client2.unsubscribe("test/a/#");
//        client2.unsubscribe("test/c/+");
        client2.disconnect();

        System.out.println("------------------------------------------------");
        boolean test1Pass1 = mqttHandler1.countMessagesByTopicEquals("test/untopic", 1);
        boolean test1Pass2 = mqttHandler1.countMessagesByTopicEquals("test/a/b", 1);
        boolean test1Pass3 = mqttHandler1.countMessagesByTopicEquals("test/c/b", 1);
        boolean test1Pass = test1Pass1 && test1Pass2 && test1Pass3;

        //N.B. client2 does not subscribd to /test/untopic
        boolean test2Pass1 = mqttHandler2.countMessagesByTopicEquals("test/untopic", 0);
        boolean test2Pass2 = mqttHandler2.countMessagesByTopicEquals("test/a/b", 1);
        boolean test2Pass3 = mqttHandler2.countMessagesByTopicEquals("test/c/b", 1);
        boolean test2Pass = test2Pass1 && test2Pass2 && test2Pass3;

        System.out.println("Test passed: "+ (test1Pass && test2Pass));
        if(test1Pass && test2Pass) {
            System.out.println("Broker supports cleanSession flag correctly.");
        }

        boolean test1bPass1 = mqttHandler1b.countMessagesByTopicEquals("test/untopic", 0);
        boolean test1bPass2 = mqttHandler1b.countMessagesByTopicEquals("test/a/b", 0);
        boolean test1bPass3 = mqttHandler1b.countMessagesByTopicEquals("test/c/b", 0);
        boolean test1bPass = test1bPass1 && test1bPass2 && test1bPass3;

        boolean test2bPass1 = mqttHandler2b.countMessagesByTopicEquals("test/untopic", 0);
        boolean test2bPass2 = mqttHandler2b.countMessagesByTopicEquals("test/a/b", 0);
        boolean test2bPass3 = mqttHandler2b.countMessagesByTopicEquals("test/c/b", 0);
        boolean test2bPass = test2bPass1 && test2bPass2 && test2bPass3;

        System.out.println("Second connect without messages: "+ test2bPass);
        if(test1bPass && test2bPass) {
            System.out.println("Second connect received no messages, it's correct!");
        }
        System.out.println("------------------------------------------------");
        System.out.println();

        allTestsPass = test1Pass && test2Pass;
    }

}
