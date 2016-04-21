package io.github.giovibal.mqtt.security.impl;

import io.github.giovibal.mqtt.security.AuthorizationClient;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;

/**
 * Created by Giovanni Baleani on 04/02/2015.
 */

public class OAuth2AuthenticatorVerticle extends AuthenticatorVerticle {

//    private static Logger logger = LoggerFactory.getLogger("mqtt-broker-log");

    private Oauth2TokenValidator oauth2Validator;

    @Override
    public void startAuthenticator(String address, AuthenticatorConfig c) throws Exception {

        String identityURL = c.getIdpUrl();
        String idp_userName = c.getIdpUsername();
        String idp_password = c.getIdpPassword();

        oauth2Validator = new Oauth2TokenValidator(identityURL, idp_userName, idp_password);

        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
            JsonObject oauth2_token = msg.body();
            String access_token = oauth2_token.getString("username");
            String refresh_token = oauth2_token.getString("password");
            // token validation
            JsonObject json = new JsonObject();
            Boolean tokanIsValid = Boolean.FALSE;
            try {
                tokanIsValid = oauth2Validator.tokenIsValid(access_token);
                TokenInfo info = oauth2Validator.getTokenInfo(access_token);
                AuthorizationClient.ValidationInfo vi = new AuthorizationClient.ValidationInfo();
                vi.auth_valid = tokanIsValid;
                vi.authorized_user = info.getAuthorizedUser();
                vi.error_msg = info.getErrorMsg();

                json = vi.toJson();
                json.put("scope", info.getScope());
                json.put("expiry_time", info.getExpiryTime());
            } catch (Exception e) {
                logger.fatal(e.getMessage(), e);
            }
            msg.reply(json);
        });

        logger.info("Startd MQTT Authorization, address: " + consumer.address());
    }
}
