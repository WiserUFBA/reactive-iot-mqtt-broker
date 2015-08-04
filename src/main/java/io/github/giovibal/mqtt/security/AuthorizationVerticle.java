package io.github.giovibal.mqtt.security;

import io.github.giovibal.mqtt.ConfigParser;
import io.github.giovibal.mqtt.Container;
import io.github.giovibal.mqtt.security.wso2.Oauth2TokenValidator;
import io.github.giovibal.mqtt.security.wso2.TokenInfo;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

/**
 * Created by giova_000 on 04/02/2015.
 */

/**
 - 1 -
 key:secret --> 4pTqLUQL0IkWa7kWEdogaVsaKKoa:l4uabj4w2e_hWqndCE43tG02qbEa -Base64 (chr type ASCII)-> NHBUcUxVUUwwSWtXYTdrV0Vkb2dhVnNhS0tvYTpsNHVhYmo0dzJlX2hXcW5kQ0U0M3RHMDJxYkVh

 - 2 -

 curl -k -d "grant_type=password&username=admin&password=admin" -H "Authorization: Basic NHBUcUxVUUwwSWtXYTdrV0Vkb2dhVnNhS0tvYTpsNHVhYmo0dzJlX2hXcW5kQ0U0M3RHMDJxYkVh, Content-Type: application/x-www-form-urlencoded" https://192.168.231.55:9443/oauth2/token
 {"token_type":"bearer","expires_in":2012,"refresh_token":"2ea51d566aa492b9edd21fe913385e76","access_token":"3419fcbea682bfe78b996eeaca1ee"}

 OPPURE ....
 curl --user 4pTqLUQL0IkWa7kWEdogaVsaKKoa:l4uabj4w2e_hWqndCE43tG02qbEa -k -d "grant_type=password&username=admin&password=admin" -H "Content-Type: application/x-www-form-urlencoded" https://192.168.231.55:9443/oauth2/token
 {"token_type":"bearer","expires_in":3299,"refresh_token":"2ea51d566aa492b9edd21fe913385e76","access_token":"3419fcbea682bfe78b996eeaca1ee"}


 - 3 -
 curl -k -H "Authorization:Bearer 3419fcbea682bfe78b996eeaca1ee" "http://192.168.231.57:8280/sp/config/graph?tenant=test"

 - 2 - altro utente -
 ALTRO UTENTE
 curl -k -d "grant_type=password&username=giovanni&password=Passw0rd" -H "Authorization: Basic NHBUcUxVUUwwSWtXYTdrV0Vkb2dhVnNhS0tvYTpsNHVhYmo0dzJlX2hXcW5kQ0U0M3RHMDJxYkVh, Content-Type: application/x-www-form-urlencoded" https://192.168.231.55:9443/oauth2/token
 {"token_type":"bearer","expires_in":3299,"refresh_token":"4a6b78f15970e67f2bfff8c312c1bc47","access_token":"8676b434c458d46e9a84303a68e4af95"}
 - 3 - altro utente -
 curl -k -H "Authorization:Bearer 8676b434c458d46e9a84303a68e4af95" "http://192.168.231.57:8280/sp/config/graph?tenant=test"
 */

public class AuthorizationVerticle extends AbstractVerticle {
    Oauth2TokenValidator oauth2Validator;
    @Override
    public void start() throws Exception {

//        String trustStorePath="C:\\Software\\WSO2\\wso2carbon.jks";
//        String trustStorePassword="wso2carbon";

//        String identityURL = "http://is.eimware.it:80";
//        String idp_userName="admin";
//        String idp_password="d0_ut_d3s$";

        ConfigParser c = new ConfigParser(config());
        boolean securityEnabled = c.isSecurityEnabled();
        String identityURL = c.getIdpUrl();
        String idp_userName = c.getIdpUsername();
        String idp_password = c.getIdpPassword();

        if(!securityEnabled) {
            Container.logger().debug("MQTT Authorization disabled");
            return;
        }

        oauth2Validator = new Oauth2TokenValidator(identityURL, idp_userName, idp_password);


        String address = AuthorizationVerticle.class.getName();

        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
            JsonObject oauth2_token = msg.body();
            String access_token = oauth2_token.getString("access_token");
            String refresh_token = oauth2_token.getString("refresh_token");

            // token validation
            JsonObject json = new JsonObject();
            Boolean tokanIsValid = Boolean.FALSE;
            try {
                tokanIsValid = oauth2Validator.tokenIsValid(access_token);
                TokenInfo info = oauth2Validator.getTokenInfo(access_token);
                json.put("token_valid", tokanIsValid);
                json.put("authorized_user", info.getAuthorizedUser());
                json.put("error_msg", info.getErrorMsg());
                json.put("scope", info.getScope());
                json.put("expiry_time", info.getExpiryTime());
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            msg.reply(json);

        });

        Container.logger().info("Startd MQTT Authorization, address: "+ consumer.address());
    }
}
