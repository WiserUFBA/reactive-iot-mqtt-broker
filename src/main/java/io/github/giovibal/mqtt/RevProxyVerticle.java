//package io.github.giovibal.mqtt;
//
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.AsyncResult;
//import io.vertx.core.DeploymentOptions;
//import io.vertx.core.Handler;
//import io.vertx.core.buffer.Buffer;
//import io.vertx.core.eventbus.Message;
//import io.vertx.core.eventbus.MessageConsumer;
//import io.vertx.core.json.JsonObject;
//import io.vertx.core.net.*;
//import io.vertx.core.streams.Pump;
//import org.dna.mqtt.moquette.proto.messages.ConnectMessage;
//
///**
// * Created by giova_000 on 29/06/2015.
// */
//public class RevProxyVerticle extends AbstractVerticle {
//
//    @Override
//    public void start() throws Exception {
//
//        JsonObject conf = new JsonObject()
//                .put("proxy.frontend.port",1883)
//                .put("proxy.backend.host","192.168.231.53")
////                .put("proxy.backend.host","localhost")
//                .put("proxy.backend.port", 1883)
//                ;
//
//        DeploymentOptions optFrontend = new DeploymentOptions().setConfig(conf).setInstances(1);
//        vertx.deployVerticle(RevProxyFrontendVerticle.class.getName(), optFrontend, stringAsyncResult -> System.out.println(stringAsyncResult.result()));
//
//
//    }
//
//}
