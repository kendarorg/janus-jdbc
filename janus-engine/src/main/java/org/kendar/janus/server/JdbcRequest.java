package org.kendar.janus.server;

import org.kendar.janus.cmd.JdbcCommand;

public class JdbcRequest {
    private JdbcCommand command;
    private Long connectionId;
    private Long uid;

    public JdbcCommand getCommand() {
        return command;
    }

    public void setCommand(JdbcCommand command) {
        this.command = command;
    }

    public Long getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(Long connectionId) {
        this.connectionId = connectionId;
    }

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }
}
