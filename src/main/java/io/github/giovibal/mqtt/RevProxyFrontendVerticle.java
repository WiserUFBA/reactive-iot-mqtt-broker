//package io.github.giovibal.mqtt;
//
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.DeploymentOptions;
//import io.vertx.core.Handler;
//import io.vertx.core.buffer.Buffer;
//import io.vertx.core.eventbus.Message;
//import io.vertx.core.net.NetClient;
//import io.vertx.core.net.NetServer;
//import io.vertx.core.net.NetServerOptions;
//import io.vertx.core.net.NetSocket;
//
//import java.util.UUID;
//
///**
// * Created by giova_000 on 29/06/2015.
// */
//public class RevProxyFrontendVerticle extends AbstractVerticle {
//
//    @Override
//    public void start() throws Exception {
//
//        // deploy backends....
//        deployBackend();
//
//        // PROXY FRONTEND
//        int proxyPort = config().getInteger("proxy.frontend.port", 1885);
//
//        NetServer netServer = vertx.createNetServer(new NetServerOptions().setPort(proxyPort));
//        netServer.connectHandler(proxyNetSocket -> {
//            NetSocketWrapper proxySocket = new NetSocketWrapper(proxyNetSocket);
//            proxyNetSocket.handler(buffer -> {
//                Container.logger().debug("MQTT Proxy from-proxy-to-backend");
//                vertx.eventBus().send("from-proxy-to-backend", buffer);
//            });
//            vertx.eventBus().consumer("from-backend-to-proxy", (Message<Buffer> objectMessage) -> {
//                Container.logger().debug("MQTT Proxy from-backend-to-proxy");
//                Buffer buff = objectMessage.body();
//                proxySocket.sendMessageToClient(buff);
//            });
//        });
//        netServer.listen();
//
//    }
//
//
//    private void deployBackend() {
//        String managerTopic = UUID.randomUUID().toString();
//        System.out.println("Deploy Backend: managerTopic = "+ managerTopic);
//        DeploymentOptions optBackend = new DeploymentOptions().setConfig(config().put("manager.topic",managerTopic)).setInstances(1);
//        vertx.deployVerticle(RevProxyBackendVerticle.class.getName(), optBackend, stringAsyncResult -> {
//            String deplID = stringAsyncResult.result();
//            System.out.println("DeploymentID Backend: "+ deplID);
//
//            vertx.eventBus().consumer(managerTopic, new Handler<Message<String>>() {
//                @Override
//                public void handle(Message<String> deplIdFromBackend) {
//                    String did = deplIdFromBackend.body();
//                    System.out.println("Undeploy Backend: "+ did);
//                    vertx.undeploy(did);
//
//                    //recursion ...?
//                    deployBackend();
//                }
//            });
//        });
//    }
//
//}
