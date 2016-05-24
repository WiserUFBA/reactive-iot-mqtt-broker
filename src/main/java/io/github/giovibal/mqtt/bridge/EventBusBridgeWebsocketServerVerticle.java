package io.github.giovibal.mqtt.bridge;

import io.github.giovibal.mqtt.Container;
import io.github.giovibal.mqtt.MQTTSession;
import io.github.giovibal.mqtt.security.CertInfo;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.parsetools.RecordParser;

/**
 * Created by Giovanni Baleani on 15/07/2015.
 */
public class EventBusBridgeWebsocketServerVerticle extends AbstractVerticle {

    private static Logger logger = LoggerFactory.getLogger(EventBusBridgeWebsocketServerVerticle.class);
    
    @Override
    public void start() throws Exception {

        JsonObject conf = config();

        String address = MQTTSession.ADDRESS;
        Integer localBridgePort = conf.getInteger("local_bridge_port", 7007);
        int idelTimeout = conf.getInteger("socket_idle_timeout", 30);


        // [WebSocket -> BUS] listen WebSocket publish to BUS
        HttpServerOptions opt = new HttpServerOptions()
                .setTcpKeepAlive(true)
                .setIdleTimeout(idelTimeout)
                .setPort(localBridgePort)
        ;

        String ssl_cert_key = conf.getString("ssl_cert_key");
        String ssl_cert = conf.getString("ssl_cert");
        String ssl_trust = conf.getString("ssl_trust");
        if(ssl_cert_key != null && ssl_cert != null && ssl_trust != null) {
            opt.setSsl(true).setClientAuth(ClientAuth.REQUIRED)
                .setPemKeyCertOptions(new PemKeyCertOptions()
                    .setKeyPath(ssl_cert_key)
                    .setCertPath(ssl_cert)
                )
                .setPemTrustOptions(new PemTrustOptions()
                    .addCertPath(ssl_trust)
                )
            ;
        }
        HttpServer netServer = vertx.createHttpServer(opt);
        netServer.websocketHandler( (ServerWebSocket webSocket) -> {
            final EventBusWebsocketBridge ebnb = new EventBusWebsocketBridge(webSocket, vertx.eventBus(), address);
            webSocket.closeHandler(aVoid -> {
                logger.info("Bridge Server - closed connection from client " + webSocket.textHandlerID());
                ebnb.stop();
            });
            webSocket.exceptionHandler(throwable -> {
                logger.error("Bridge Server - Exception: " + throwable.getMessage(), throwable);
                ebnb.stop();
            });

            logger.info("Bridge Server - new connection from client " + webSocket.textHandlerID());

            final RecordParser parser = RecordParser.newDelimited("\n", h -> {
                String cmd = h.toString();
                if ("START SESSION".equalsIgnoreCase(cmd)) {
                    webSocket.pause();
                    ebnb.start();
                    logger.info("Bridge Server - bridgeUUID: " + ebnb.getBridgeUUID());
                    webSocket.resume();
                } else {
                    String tenant = cmd;
                    String tenantFromCert = new CertInfo(webSocket).getTenant();
//                    if(!tenant.equals(tenantFromCert))
//                        throw new IllegalAccessError("Bridge Authentication Failed for tenant: "+ tenant +"/"+ tenantFromCert);
                    if(tenantFromCert != null)
                        tenant = tenantFromCert;

                    ebnb.setTenant(tenant);
                }
            });
            webSocket.handler(parser::handle);

        }).listen();
    }

    @Override
    public void stop() throws Exception {

    }
}
