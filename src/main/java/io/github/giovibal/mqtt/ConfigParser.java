package io.github.giovibal.mqtt;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by giova_000 on 23/02/2015.
 */
public class ConfigParser {

    private int port;
//    private int wsPort;
    private boolean wsEnabled;
    private String wsSubProtocols;
    private boolean retainSupport;

    private boolean securityEnabled;
    private List<String> authorizedClients;
    private String idpUrl;
    private String idpUsername;
    private String idpPassword;

    private String tlsKeyPath;
    private String tlsCertPath;


    public ConfigParser() { }

    public ConfigParser(JsonObject conf) {
        parse(conf);
    }
    public void parse(JsonObject conf) {
        port = conf.getInteger("tcp_port", 1883);
//        wsPort = conf.getInteger("websocket_port", 11883);
        wsEnabled = conf.getBoolean("websocket_enabled", false);
        wsSubProtocols = conf.getString("websocket_subprotocols", "mqtt,mqttv3.1");
        retainSupport = conf.getBoolean("retain_support", true);

        JsonObject security = conf.getJsonObject("security", new JsonObject());
        securityEnabled = security.getBoolean("enabled", false);
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

        JsonObject tls = conf.getJsonObject("tls", new JsonObject());
        tlsKeyPath = tls.getString("keyPath");
        tlsCertPath = tls.getString("certPath");
    }

    public int getPort() {
        return port;
    }

//    public int getWsPort() {
//        return wsPort;
//    }

    public boolean isWsEnabled() {
        return wsEnabled;
    }

    public String getWsSubProtocols() {
        return wsSubProtocols;
    }

    public boolean isSecurityEnabled() {
        return securityEnabled;
    }

    public List<String> getAuthorizedClients() {
        return authorizedClients;
    }

    public String getIdpUrl() {
        return idpUrl;
    }

    public String getIdpUsername() {
        return idpUsername;
    }

    public String getIdpPassword() {
        return idpPassword;
    }

    public boolean isAuthorizedClient(String clientID) {
        if(authorizedClients!=null) {
            for(String ac : authorizedClients) {
                if(clientID.matches(ac)) {
                    return true;
                }
            }
        }
        return false;
    }

    public String getTlsKeyPath() {
        return tlsKeyPath;
    }

    public String getTlsCertPath() {
        return tlsCertPath;
    }

    public boolean isTlsEnabled() {
        String keyPath = getTlsKeyPath();
        String certPath = getTlsCertPath();
        boolean ret = keyPath!=null && keyPath.trim().length()>0 && certPath!=null && certPath.trim().length()>0;
        return ret;
    }

    public boolean isRetainSupport() {
        return retainSupport;
    }

    public List<String> getFeatures() {
        List<String> ret = new ArrayList<>();
        if(wsEnabled)
            ret.add("websocket");
        if(securityEnabled)
            ret.add("security");
        if(!retainSupport)
            ret.add("retain_disabled");
        return ret;
    }
    public String getFeatursInfo() {
        StringBuilder ret = new StringBuilder();
        List<String> features = getFeatures();
        for(String f : features) {
            ret.append(f).append(",");
        }
        String s = ret.toString();
        int idx = s.lastIndexOf(',');
        if(idx>=0)
            s = s.substring(0, idx);
        return s;
    }
}
