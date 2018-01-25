package br.ufba.dcc.wiser;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AbstractVerticle;


import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.MqttTopicSubscription;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 *
 * @author Cleber Lira
 * @version 1.0
 * @since 01242018
 */


public class VertxMqtt extends AbstractVerticle {

    public static final String MQTT_SERVER_HOST = "localhost";
    public static final int MQTT_SERVER_PORT = 1883;

    private static final Logger LOG = LoggerFactory.getLogger(VertxMqtt.class);

    private static void handleSubscription(MqttEndpoint endpoint) {
        endpoint.subscribeHandler(subscribe -> {

            List grantedQosLevels = new ArrayList<>();
            for (MqttTopicSubscription s : subscribe.topicSubscriptions()) {

                LOG.info("Subscription for " + s.topicName() + " with QoS " + s.qualityOfService());
                System.out.println("Subscription for " + s.topicName() + " with QoS " + s.qualityOfService());
                
                grantedQosLevels.add(s.qualityOfService());
            }

            endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);

        });
    }

    private static void handleUnsubscription(MqttEndpoint endpoint) {
        endpoint.unsubscribeHandler(unsubscribe -> {

            for (String t : unsubscribe.topics()) {
                LOG.info("Unsubscription for " + t);
            }

            endpoint.unsubscribeAcknowledge(unsubscribe.messageId());
        });
    }

    private static void publishHandler(MqttEndpoint endpoint) {
        endpoint.publishHandler(message -> {

            LOG.info("Just received message [" + message.payload().toString(Charset.defaultCharset()) + "] with QoS [" + message.qosLevel() + "]");
            System.out.println("Just received message [" + message.payload().toString(Charset.defaultCharset()) + "] with QoS [" + message.qosLevel() + "]");
            
            
            if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
                endpoint.publishAcknowledge(message.messageId());
            } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {
                endpoint.publishRelease(message.messageId());
            }

        }).publishReleaseHandler(messageId -> {

            endpoint.publishComplete(messageId);
        });
    }

    private static void handleClientDisconnect(MqttEndpoint endpoint) {
        endpoint.disconnectHandler(h -> {
            LOG.info("The remote client has closed the connection.");
        });
    }

   
    @Override
    public void start() {

        MqttServerOptions opts = new MqttServerOptions();
        opts.setHost(MQTT_SERVER_HOST);
        opts.setPort(MQTT_SERVER_PORT);

        MqttServer mqttServer = MqttServer.create(vertx, opts);

        mqttServer.endpointHandler(endpoint -> {
            // accept connection from the remote client
            endpoint.accept(false);

            handleSubscription(endpoint);
            handleUnsubscription(endpoint);
            publishHandler(endpoint);
            handleClientDisconnect(endpoint);
            // shows main connect info
            LOG.info("MQTT client [" + endpoint.clientIdentifier() + "] request to connect, clean session = " + endpoint.isCleanSession());

            if (endpoint.auth() != null) {
                System.out.println("[username = " + endpoint.auth().userName() + ", password = " + endpoint.auth().password() + "]");
            }
            if (endpoint.will() != null) {
                System.out.println("[will topic = " + endpoint.will().willTopic() + " msg = " + endpoint.will().willMessage()
                        + " QoS = " + endpoint.will().willQos() + " isRetain = " + endpoint.will().isWillRetain() + "]");
            }

            LOG.info("[keep alive timeout = " + endpoint.keepAliveTimeSeconds() + "]");

        })
                .listen(ar -> {

                    if (ar.succeeded()) {

                        LOG.info("MQTT server is listening on port " + ar.result().actualPort());
                        System.out.println("MQTT server is listening on port " + ar.result().actualPort());
                    
                    } else {

                        LOG.error("Error on starting the server");
                        ar.cause().printStackTrace();
                    }
                });

    }
}
