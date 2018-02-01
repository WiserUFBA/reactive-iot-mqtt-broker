package br.ufba.dcc.wiser;

import static br.ufba.dcc.wiser.util.Calable.executeWithTCCLSwitch;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;

import io.vertx.mqtt.MqttEndpoint;

import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.MqttTopicSubscription;

import java.nio.charset.Charset;
import java.util.ArrayList;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.felix.ipojo.annotations.*;

/**
 *
 *
 *
 * @author Cleber Lira
 * @version 1.0
 * @since 01242018
 */

@Component(immediate = true)
@Instantiate
public class VertxMqtt {

    public static final String MQTT_SERVER_HOST = "localhost";
    public static final int MQTT_SERVER_PORT = 1883;

   

    private static final Logger LOG = LoggerFactory.getLogger(VertxMqtt.class);

    public void init() {
        System.out.println("Inicializando Vertx");
    }

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

        System.out.println("MQTT client [{}] connected" + endpoint.clientIdentifier());

        LOG.info("MQTT client [{}] connected", endpoint.clientIdentifier());

        endpoint.publishHandler(message -> {

            LOG.info("Just received message [" + message.payload().toString(Charset.defaultCharset()) + "] with QoS [" + message.qosLevel() + "]");
            System.out.println("Just received message [" + message.payload().toString(Charset.defaultCharset()) + "] with QoS [" + message.qosLevel() + "]");

            //  vert.eventBus().publish("REACTIVE", String.valueOf(message.payload()));
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
            System.out.println("The remote client has closed the connection.");
            LOG.info("The remote client has closed the connection.");
        });
    }

    @Validate
    public void test() throws Exception {

        MqttServerOptions opts = new MqttServerOptions();
        opts.setHost(MQTT_SERVER_HOST);
        opts.setPort(MQTT_SERVER_PORT);

        Vertx vertx = executeWithTCCLSwitch(() -> Vertx.vertx());
        
        System.out.println("MqttServer" + vertx);

        MqttServer mqttServer = executeWithTCCLSwitch(() -> MqttServer.create(vertx, opts));

        System.out.println("star conectadno");
        mqttServer.endpointHandler(endpoint -> {

            System.out.println("star conectadno 2");
            // accept connection from the remote client
            endpoint.accept(false);

            handleSubscription(endpoint);

            System.out.println("star conectadno 3");
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

       // vertx.close();
    }

}
