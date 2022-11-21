package org.kendar.janus.server;

import org.kendar.janus.results.JdbcResult;

public class ResponseRequest {
    private JdbcRequest request;
    private JdbcResult response;

    public JdbcRequest getRequest() {
        return request;
    }

    public void setRequest(JdbcRequest request) {
        this.request = request;
    }

    public JdbcResult getResponse() {
        return response;
    }

    public void setResponse(JdbcResult response) {
        this.response = response;
    }
}
