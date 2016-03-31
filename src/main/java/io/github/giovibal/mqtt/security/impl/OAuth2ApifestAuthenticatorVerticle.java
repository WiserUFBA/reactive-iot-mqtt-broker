package io.github.giovibal.mqtt.security.impl;

import io.github.giovibal.mqtt.security.AuthorizationClient;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.net.URL;

/**
 * Created by Giovanni Baleani on 04/02/2015.
 */

/**
 GET http://127.0.0.1:8080/oauth20/tokens/validate?token=f48db3829c71b9dc4957e3bb7b804bd0d44db10a2b9e30346796c2d9e9f44722

 Response:

 Status Code: 200 OK
 Content-Type: application/json
 {
     "token": "f48db3829c71b9dc4957e3bb7b804bd0d44db10a2b9e30346796c2d9e9f44722",
     "refreshToken": "fe45a6ea3d7a0f2cacc864523ab586551739bcbe1a167fe1d37edc6b004ad452",
     "expiresIn": "120",
     "type": "Bearer",
     "scope": "test_scope",
     "valid": true,
     "clientId": "b9db6d84dc98a895035e68f972e30503d3c724c8",
     "codeId": "",
     "userId": "12345",
     "created": 1432542201299,
     "refreshExpiresIn": "300"
 }
 */

public class OAuth2ApifestAuthenticatorVerticle extends AbstractAuthenticatorVerticle {

    private static Logger logger = LoggerFactory.getLogger("mqtt-broker-log");

    @Override
    public void startAuthenticator(String address, JsonObject conf) throws Exception {

        SecurityConfigParser c = new SecurityConfigParser(conf);
        String identityURL = c.getIdpUrl();
        String idp_userName = c.getIdpUsername();
        String idp_password = c.getIdpPassword();

        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
            JsonObject oauth2_token = msg.body();
            String access_token = oauth2_token.getString("username");
            String refresh_token = oauth2_token.getString("password");

            // token validation
            try {
                HttpClientOptions opt = new HttpClientOptions();
                HttpClient httpClient = vertx.createHttpClient(opt);
                URL url = new URL(identityURL);

                String uri = url.getPath() + "/validate?token=" + access_token;
                httpClient.get(url.getPort(), url.getHost(), uri, resp -> {
                    resp.exceptionHandler(e -> {
                        logger.fatal(e.getMessage(), e);

                        AuthorizationClient.ValidationInfo vi = new AuthorizationClient.ValidationInfo();
                        vi.auth_valid = false;
                        vi.authorized_user = "";
                        vi.error_msg = e.getMessage();
                        msg.reply(vi.toJson());
                    });
                    resp.bodyHandler(totalBuffer -> {
                        String jsonResponse = totalBuffer.toString("UTF-8");
                        logger.info(jsonResponse);

                        JsonObject j = new JsonObject(jsonResponse);
                        String token = j.getString("token");
                        String refreshToken = j.getString("refreshToken");
                        String expiresIn = j.getString("expiresIn");
                        String type = j.getString("type");
                        String scope = j.getString("scope");
                        boolean valid = j.getBoolean("valid", false);
                        String clientId = j.getString("clientId");
                        String codeId = j.getString("codeId");
                        String userId = j.getString("userId");
                        Long created = j.getLong("created");
                        String refreshExpiresIn = j.getString("refreshExpiresIn");

                        AuthorizationClient.ValidationInfo vi = new AuthorizationClient.ValidationInfo();
                        vi.auth_valid = valid;
                        vi.authorized_user = userId;
                        vi.error_msg = "";

                        JsonObject json = new JsonObject();
                        json = vi.toJson();
                        json.put("scope", scope);
                        json.put("expiry_time", expiresIn);

                        msg.reply(json);
                    });
                }).end();
            } catch (Throwable e) {
                logger.fatal(e.getMessage(), e);

                AuthorizationClient.ValidationInfo vi = new AuthorizationClient.ValidationInfo();
                vi.auth_valid = false;
                vi.authorized_user = "";
                vi.error_msg = e.getMessage();
                msg.reply(vi.toJson());
            }
        });

        logger.info("Startd MQTT Authorization, address: " + consumer.address());
    }

}
