package io.github.giovibal.mqtt.security.impl;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Created by giova_000 on 15/10/2015.
 */
public abstract class AbstractAuthenticatorVerticle extends AbstractVerticle {

    protected static Logger logger = LoggerFactory.getLogger("mqtt-broker-auth-log");

    @Override
    public void start() throws Exception {

        String address = config().getString("address");
        if(address!=null && address.trim().length()>0) {
            startAuthenticator(address, config());
        }

    }

    public abstract void startAuthenticator(String address, JsonObject config) throws Exception;
}
