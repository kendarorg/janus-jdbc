package org.kendar.janus.server;

import java.sql.Connection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class JdbcContext extends HashMap<Long,Object> {
    private Connection connection;
    private AtomicLong expiration= new AtomicLong();


    private AtomicLong counter = new AtomicLong(1L);

    public void updateExpiration(long timeout){
        expiration.set(timeout);
    }


    public boolean isExpired(long time) {
        return time>expiration.get();
    }

    public long getNext(){
        return counter.incrementAndGet();
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
