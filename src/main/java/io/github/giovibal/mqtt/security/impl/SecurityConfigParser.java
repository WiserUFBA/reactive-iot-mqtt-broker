package io.github.giovibal.mqtt.security.impl;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by giova_000 on 23/02/2015.
 */
public class SecurityConfigParser {

//    private boolean securityEnabled;
    private List<String> authorizedClients;
    private String idpUrl;
    private String idpUsername;
    private String idpPassword;

    SecurityConfigParser(JsonObject conf) {
        parse(conf);
    }
    void parse(JsonObject conf) {
        JsonObject security = conf.getJsonObject("security", new JsonObject());
//        securityEnabled = security.getBoolean("enabled", false);
        JsonArray authorizedClientsArr = security.getJsonArray("authorized_clients", new JsonArray().add("testing.*"));
        if(authorizedClientsArr != null) {
            authorizedClients = new ArrayList<>();
            for(int i=0; i<authorizedClientsArr.size(); i++) {
                String item = authorizedClientsArr.getString(i);
                authorizedClients.add(item);
            }
        }
        idpUrl = security.getString("idp_url", "http://192.168.231.55:9763");
        idpUsername = security.getString("idp_username", "admin");
        idpPassword = security.getString("idp_password", "admin");
    }


//    boolean isSecurityEnabled() {
//        return securityEnabled;
//    }

    List<String> getAuthorizedClients() {
        return authorizedClients;
    }

    String getIdpUrl() {
        return idpUrl;
    }

    String getIdpUsername() {
        return idpUsername;
    }

    String getIdpPassword() {
        return idpPassword;
    }

    boolean isAuthorizedClient(String clientID) {
        if(authorizedClients!=null) {
            for(String ac : authorizedClients) {
                if(clientID.matches(ac)) {
                    return true;
                }
            }
        }
        return false;
    }
}
