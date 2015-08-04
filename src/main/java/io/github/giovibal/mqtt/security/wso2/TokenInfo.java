package io.github.giovibal.mqtt.security.wso2;

import java.util.List;

/**
 * Created by giova_000 on 19/02/2015.
 */
public class TokenInfo {
    private String authorizedUser;
    private List<String> scope;
    private Long expiryTime;
    private String errorMsg;

    public String getAuthorizedUser() {
        return authorizedUser;
    }

    public void setAuthorizedUser(String authorizedUser) {
        this.authorizedUser = authorizedUser;
    }

    public List<String> getScope() {
        return scope;
    }

    public void setScope(List<String> scope) {
        this.scope = scope;
    }

    public Long getExpiryTime() {
        return expiryTime;
    }

    public void setExpiryTime(Long expiryTime) {
        this.expiryTime = expiryTime;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    @Override
    public String toString() {
        return authorizedUser +" ["+expiryTime+" "+scope+" "+errorMsg+"]";
    }
}
