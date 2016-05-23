package io.github.giovibal.mqtt.bridge;

import io.github.giovibal.mqtt.Container;
import io.github.giovibal.mqtt.MQTTSession;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;

/**
 * Created by giova_000 on 15/07/2015.
 */
public class EventBusBridgeWebsocketClientVerticle extends AbstractVerticle implements Handler<WebSocket> {

    private HttpClient netClient;
    private String remoteBridgeHost;
    private Integer remoteBridgePort;
    private String address;
    private long connectionTimerID;
    private boolean connected;
    private String tenant;

    @Override
    public void start() throws Exception {

        JsonObject conf = config();

        remoteBridgeHost = conf.getString("remote_bridge_host", "192.168.231.53");
        remoteBridgePort = conf.getInteger("remote_bridge_port", 7007);
        address = MQTTSession.ADDRESS;
        tenant = conf.getString("remote_bridge_tenant", "cmroma.it");

        // [TCP <- BUS] listen BUS write to TCP
        int timeout = 1000;
//        NetClientOptions opt = new NetClientOptions()
//                .setConnectTimeout(timeout) // 60 seconds
//                .setIdleTimeout(10) // 1 second
//                .setTcpKeepAlive(true)
////                .setSsl(true)
////                .setPemKeyCertOptions(new PemKeyCertOptions()
////                    .setKeyPath("C:\\Sviluppo\\Certificati-SSL\\cmroma.it\\cmroma.it_pkcs8.key")
////                    .setCertPath("C:\\Sviluppo\\Certificati-SSL\\cmroma.it\\cmroma.it.crt")
////                )
////                .setPemTrustOptions(new PemTrustOptions()
////                    .addCertPath("C:\\Sviluppo\\Certificati-SSL\\CA\\rootCA.pem")
////                )
//            ;

        HttpClientOptions opt = new HttpClientOptions()
                .setConnectTimeout(timeout) // 60 seconds
                .setIdleTimeout(10) // 1 second
                .setTcpKeepAlive(true)
            ;

        netClient = vertx.createHttpClient(opt);
        //netClient.connect(remoteBridgePort, remoteBridgeHost, this);
        netClient.websocket(remoteBridgePort, remoteBridgeHost, "/bridge", this);
        connectionTimerID = vertx.setPeriodic(timeout*2, aLong -> {
            checkConnection();
        });
    }

    private void checkConnection() {
        if(!connected) {
            Container.logger().info("Bridge Client - try to reconnect to server [" + remoteBridgeHost + ":" + remoteBridgePort + "] ... " + connectionTimerID);
//            netClient.connect(remoteBridgePort, remoteBridgeHost, this);
            netClient.websocket(remoteBridgePort, remoteBridgeHost, "/bridge", this);
        }
    }

    @Override
    public void handle(WebSocket netSocketAsyncResult) {
//        if (netSocketAsyncResult.succeeded()) {
            connected = true;
            Container.logger().info("Bridge Client - connected to server [" + remoteBridgeHost + ":" + remoteBridgePort + "]");
//            WebSocket webSocket = netSocketAsyncResult.result();
            WebSocket webSocket = netSocketAsyncResult;
            webSocket.closeHandler(aVoid -> {
                Container.logger().error("Bridge Client - closed connection from server [" + remoteBridgeHost + ":" + remoteBridgePort + "]" + webSocket.textHandlerID());
                connected = false;
            });
            webSocket.exceptionHandler(throwable -> {
                Container.logger().error("Bridge Client - Exception: " + throwable.getMessage(), throwable);
                connected = false;
            });

            webSocket.writeFinalTextFrame(tenant + "\n");
            webSocket.writeFinalTextFrame("START SESSION" + "\n");
            webSocket.pause();
            EventBusWebsocketBridge ebnb = new EventBusWebsocketBridge(webSocket, vertx.eventBus(), address);
            ebnb.setTenant(tenant);
            ebnb.start();
            Container.logger().info("Bridge Client - bridgeUUID: "+ ebnb.getBridgeUUID());
            webSocket.resume();
//        } else {
//            connected = false;
//            String msg = "Bridge Client - not connected to server [" + remoteBridgeHost + ":" + remoteBridgePort +"]";
//            Throwable e = netSocketAsyncResult.cause();
//            if (e != null) {
//                Container.logger().error(msg, e);
//            } else {
//                Container.logger().error(msg);
//            }
//        }
    }

    @Override
    public void stop() throws Exception {
        vertx.cancelTimer(connectionTimerID);
        connected = false;
    }

}
