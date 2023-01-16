package org.kendar.janus.utils;

import java.sql.Connection;

public class TimedOutConnection {
    private Connection connection;
    private long expiration;

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }
}
