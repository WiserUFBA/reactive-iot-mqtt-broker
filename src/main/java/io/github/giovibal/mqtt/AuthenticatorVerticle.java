package io.github.giovibal.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;

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
        String appKeySecretB64 = Base64.getEncoder().encodeToString(appKeySecret.getBytes("ASCII"));

//        String url = "https://192.168.231.55:9443/oauth2/token";
//        String url = "http://192.168.231.55:9763/oauth2/token";
        String url = "http://is.eimware.it:80/oauth2/token";

        String address = MQTTBroker.class.getName()+"_auth";

        vertx.eventBus().consumer(address, (Message<JsonObject> msg) -> {
            JsonObject credentials = msg.body();
            System.out.println(credentials.toString());
            String username = credentials.getString("username");
            String password = credentials.getString("password");
            System.out.println(username + " " + password);

//            HttpClientOptions opt = new HttpClientOptions()
//                    .setTrustAll(true)
//                ;

//            HttpClient httpClient = vertx.createHttpClient(opt);
            HttpClient httpClient = vertx.createHttpClient();
            HttpClientRequest request = httpClient.postAbs(url);
            request.handler(response -> {
                System.out.println(response.statusCode());
                response.bodyHandler(buffer -> {
                    String body = new String(buffer.getBytes());
                    System.out.println(body);
                    JsonObject json = new JsonObject(body);
                    String refresh_token = json.getString("refresh_token");
                    String access_token = json.getString("access_token");

                    // token validation
//                    boolean tokanIsValid = false;
//                    try {
////                        HttpClientRequest validationReq = httpClient.getAbs("http://is.eimware.it:80/oauth2/userinfo?schema=openid");
//                        HttpClientRequest validationReq = httpClient.get(80, "is.eimware.it", "/oauth2/userinfo?schema=openid");
//                        validationReq.handler(validationResp -> {
//                            System.out.println("validationResp.statusCode ===> "+ validationResp.statusCode());
//                            validationResp.bodyHandler(validationBuffer -> {
//                                String validationBody = new String(validationBuffer.getBytes());
//                                System.out.println("validationBody ===> " + validationBody);
//                            });
//                        });
//                        validationReq.exceptionHandler(e -> {
//                            System.out.println("Received exception: " + e.getMessage());
//                            e.printStackTrace();
//                        });
//
//                        validationReq.putHeader("Authorization", "Bearer "+ access_token);
//                        System.out.printf("Bearer "+ access_token);
//                        validationReq.end();
//
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }

                    if(access_token!=null) {
                        msg.reply(new JsonObject().put("authenticated", true).put("auth_info", json));
                    }
                    else {
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
            String data = "grant_type=password&username="+username+"&password="+password+"&scope=openid";
            request.end(data);


//            msg.reply(new JsonObject().put("authenticated", true));
        });

        Container.logger().info("Startd MQTT Authenticator");
    }
}
