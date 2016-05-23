package io.github.giovibal.mqtt.bridge;

import io.github.giovibal.mqtt.Container;
import io.github.giovibal.mqtt.MQTTSession;
import io.github.giovibal.mqtt.security.CertInfo;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;

/**
 * Created by Giovanni Bleani on 15/07/2015.
 */
public class EventBusBridgeWebsocketClientVerticle extends AbstractVerticle implements Handler<WebSocket> {

    private HttpClient netClient;
    private String remoteBridgeHost;
    private Integer remoteBridgePort;
    private String remoteBridgePath;
    private String address;
    private long connectionTimerID;
    private boolean connected;
    private String tenant;

    @Override
    public void start() throws Exception {

        JsonObject conf = config();

        remoteBridgeHost = conf.getString("remote_bridge_host", "iot.eimware.it");
        remoteBridgePort = conf.getInteger("remote_bridge_port", 7007);
        remoteBridgePath = conf.getString("remote_bridge_path", "/");
        address = MQTTSession.ADDRESS;
        tenant = conf.getString("remote_bridge_tenant");

        // [WebSocket <- BUS] listen BUS write to WebSocket
        int timeout = 1000;
        HttpClientOptions opt = new HttpClientOptions()
                .setConnectTimeout(timeout) // 60 seconds
                .setTcpKeepAlive(true)
                ;

        String ssl_cert_key = conf.getString("ssl_cert_key");
        String ssl_cert = conf.getString("ssl_cert");
        String ssl_trust = conf.getString("ssl_trust");
        if(ssl_cert_key != null && ssl_cert != null && ssl_trust != null) {
            opt.setSsl(true)
                    .setPemKeyCertOptions(new PemKeyCertOptions()
                                    .setKeyPath(ssl_cert_key)
                                    .setCertPath(ssl_cert)
                    )
                    .setPemTrustOptions(new PemTrustOptions()
                                    .addCertPath(ssl_trust)
                    )
            ;
            tenant = new CertInfo(ssl_cert).getTenant();
        }

        netClient = vertx.createHttpClient(opt);
        netClient.websocket(remoteBridgePort, remoteBridgeHost, remoteBridgePath, this);
        connectionTimerID = vertx.setPeriodic(timeout*2, aLong -> {
            checkConnection();
        });
    }

    private void checkConnection() {
        if(!connected) {
            Container.logger().info("Bridge Client - try to reconnect to server [" + remoteBridgeHost + ":" + remoteBridgePort + "] ... " + connectionTimerID);
            netClient.websocket(remoteBridgePort, remoteBridgeHost, remoteBridgePath, this);
        }
    }

    @Override
    public void handle(WebSocket webSocket) {
//        if (netSocketAsyncResult.succeeded()) {
            connected = true;
            Container.logger().info("Bridge Client - connected to server [" + remoteBridgeHost + ":" + remoteBridgePort + "]");
//            WebSocket webSocket = netSocketAsyncResult.result();
            webSocket.closeHandler(aVoid -> {
                Container.logger().error("Bridge Client - closed connection from server [" + remoteBridgeHost + ":" + remoteBridgePort + "]" + webSocket.textHandlerID());
                connected = false;
            });
            webSocket.exceptionHandler(throwable -> {
                Container.logger().error("Bridge Client - Exception: " + throwable.getMessage(), throwable);
                connected = false;
            });

            webSocket.write(Buffer.buffer( tenant + "\n" ));
            webSocket.write(Buffer.buffer( "START SESSION" + "\n" ));
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
