package io.github.giovibal.mqtt.security;

import io.github.giovibal.mqtt.Container;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Created by giova_000 on 14/10/2015.
 */
public class AuthorizationClient {

    private EventBus eventBus;
    private String authenticatorAddress;
//    private Handler<Boolean> handler;

    public AuthorizationClient(EventBus eventBus, String authenticatorAddress) {
        this.eventBus = eventBus;
        this.authenticatorAddress = authenticatorAddress;
    }

    public void authorize(String username, String password, Handler<ValidationInfo> authHandler) {
        // AUTHENTICATION START
        JsonObject oauth2_token_info = new JsonObject()
                .put("username", username)
                .put("password", password);
        eventBus.send(authenticatorAddress, oauth2_token_info, (AsyncResult<Message<JsonObject>> res) -> {
            ValidationInfo vi = new ValidationInfo();
            if (res.succeeded()) {
                JsonObject validationInfo = res.result().body();
                Container.logger().debug(validationInfo);
                vi.fromJson(validationInfo);
                Container.logger().debug("authenticated ===> " + vi.auth_valid);
                if (vi.auth_valid) {
                    Container.logger().debug("authorized_user ===> " + vi.authorized_user + ", tenant ===> " + vi.tenant);
                    authHandler.handle(vi);
                } else {
                    Container.logger().debug("authenticated error ===> " + vi.error_msg);
                    authHandler.handle(vi);
                }
            } else {
                Container.logger().debug("login failed !");
                vi.auth_valid = Boolean.FALSE;
                authHandler.handle(vi);
            }
        });
    }


    public static class ValidationInfo {
        public Boolean auth_valid;
        public String authorized_user;
        public String error_msg;
        public String tenant;
        public void fromJson(JsonObject validationInfo) {
            auth_valid = validationInfo.getBoolean("auth_valid", Boolean.FALSE);
            authorized_user = validationInfo.getString("authorized_user");
            error_msg = validationInfo.getString("error_msg");
            if (auth_valid) {
                tenant = extractTenant(authorized_user);
            }
        }
        public JsonObject toJson() {
            JsonObject json = new JsonObject();
            json.put("auth_valid", auth_valid);
            json.put("authorized_user", authorized_user);
            json.put("error_msg", error_msg);
            return json;
        }
        private String extractTenant(String username) {
            if(username == null || username.trim().length()==0)
                return "";
            String tenant = "";
            int idx = username.lastIndexOf('@');
            if(idx > 0) {
                tenant = username.substring(idx+1);
            }
            return tenant;
        }
    }



}
