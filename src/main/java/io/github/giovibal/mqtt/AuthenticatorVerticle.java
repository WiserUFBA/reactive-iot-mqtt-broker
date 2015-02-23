package io.github.giovibal.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import it.filippetti.smartplatform.oauth2.Oauth2TokenValidator;
import it.filippetti.smartplatform.oauth2.TokenInfo;

import java.util.Base64;

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

public class AuthenticatorVerticle extends AbstractVerticle {
    @Override
    public void start() throws Exception {

//        String appKeySecret = "4pTqLUQL0IkWa7kWEdogaVsaKKoa:l4uabj4w2e_hWqndCE43tG02qbEa";
        String appKeySecret = "G_wdzI2fzCPB18cGqC25bssaruQa:njuYeh34S7cDiUKsH9AjyXRkxrQa";
//        String appKeySecret = "IvogIPvV2rg3frp_mj6fTYJtAFEa:OkiTdiiR8h8WfBjhmGbGUDS7hHQa";
        String appKeySecretB64 = Base64.getEncoder().encodeToString(appKeySecret.getBytes("ASCII"));

//        String url = "https://192.168.231.55:9443/oauth2/token";
//        String url = "http://192.168.231.55:9763/oauth2/token";
        String url = "http://is.eimware.it:80/oauth2/token";
//        String url = "http://api.eimware.it:80/is/token";

        String address = AuthenticatorVerticle.class.getName();

        vertx.eventBus().localConsumer(address, (Message<JsonObject> msg) -> {
            JsonObject credentials = msg.body();
            String username = credentials.getString("username");
            String password = credentials.getString("password");

            HttpClientOptions opt = new HttpClientOptions()
                    .setTrustAll(true);
            HttpClient httpClient = vertx.createHttpClient(opt);
            HttpClientRequest request = httpClient.request(HttpMethod.POST, url);
            request.handler(response -> {
//                System.out.println(response.statusCode());
                response.bodyHandler(buffer -> {
                    String body = new String(buffer.getBytes());
                    JsonObject json = new JsonObject(body);
                    String refresh_token = json.getString("refresh_token");
                    String access_token = json.getString("access_token");

                    // token validation
//                    vertx.eventBus().send(AuthorizationVerticle.class.getName(), json, new AsyncResultHandler<Message<JsonObject>>() {
//                        @Override
//                        public void handle(AsyncResult<Message<JsonObject>> validationResult) {
//                            if(validationResult.succeeded()) {
//                                JsonObject validationInfo = validationResult.result().body();
//                                System.out.println(validationInfo);
//                                String authorized_user = validationInfo.getString("authorized_user");
//
//                            } else {
//                                Container.logger().fatal(validationResult.cause().getMessage(), validationResult.cause());
//                            }
//                        }
//                    });
                    if(access_token != null && access_token.trim().length()>0) {
                        msg.reply(new JsonObject().put("authenticated", true).put("auth_info", json));
                    } else {
                        msg.reply(new JsonObject().put("authenticated", false).put("auth_info", json));
                    }
                });
            });
            request.exceptionHandler(e -> {
                System.out.println("Received exception: " + e.getMessage());
                e.printStackTrace();
            });

            request.putHeader("Authorization", "Basic " + appKeySecretB64);
            request.putHeader("Content-Type", "application/x-www-form-urlencoded");
            String data = "grant_type=password&username=" + username + "&password=" + password + "&scope=openid";
            request.end(data);


//            msg.reply(new JsonObject().put("authenticated", true));
        });

        Container.logger().info("Startd MQTT Authenticator");
    }
}
