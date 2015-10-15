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
    private String authorizationAddress;
    private Handler<Boolean> handler;

    public AuthorizationClient(EventBus eventBus, String authorizationAddress) {
        this.eventBus = eventBus;
        this.authorizationAddress = authorizationAddress;
    }

    public void authorize(String username, String password, Handler<ValidationInfo> authHandler) {
        // AUTHENTICATION START
        JsonObject oauth2_token_info = new JsonObject()
                .put("access_token", username)
                .put("refresh_token", password);
        eventBus.send(authorizationAddress, oauth2_token_info, (AsyncResult<Message<JsonObject>> res) -> {
            ValidationInfo vi = new ValidationInfo();
            if (res.succeeded()) {
                JsonObject validationInfo = res.result().body();
                Container.logger().debug(validationInfo);
                vi.from(validationInfo);
                Container.logger().debug("authenticated ===> " + vi.token_valid);
                if (vi.token_valid) {
                    Container.logger().debug("authorized_user ===> " + vi.authorized_user + ", tenant ===> " + vi.tenant);
                    authHandler.handle(vi);
                } else {
                    Container.logger().debug("authenticated error ===> " + vi.error_msg);
                    authHandler.handle(vi);
                }
            } else {
                Container.logger().debug("login failed !");
                vi.token_valid = Boolean.FALSE;
                authHandler.handle(vi);
            }
        });
    }


    public class ValidationInfo {
        public Boolean token_valid;
        public String authorized_user;
        public String error_msg;
        public String tenant;
        void from(JsonObject validationInfo) {
            token_valid = validationInfo.getBoolean("token_valid", Boolean.FALSE);
            authorized_user = validationInfo.getString("authorized_user");
            error_msg = validationInfo.getString("error_msg");
            if (token_valid) {
                tenant = extractTenant(authorized_user);
            }
        }
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
