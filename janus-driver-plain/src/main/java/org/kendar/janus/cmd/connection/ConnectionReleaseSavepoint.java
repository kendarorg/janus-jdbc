package org.kendar.janus.cmd.connection;

import org.kendar.janus.TraceAwareType;
import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

public class ConnectionReleaseSavepoint implements JdbcCommand, TraceAwareType {

    private long traceId;

    public ConnectionReleaseSavepoint(){

    }
    public ConnectionReleaseSavepoint(long traceId){

        this.traceId = traceId;
    }
    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var connection = (Connection)context.getConnection();
        var savePoint = (Savepoint)context.get(traceId);
        connection.releaseSavepoint(savePoint);
        context.remove(traceId);
        return null;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("traceId",traceId);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        traceId = builder.read("traceId");
        return this;
    }
    @Override
    public String getPath() {
        return "/Connection/releaseSavepoint";
    }

    @Override
    public long getTraceId() {
        return traceId;
    }

    @Override
    public void setTraceId(long traceId) {
        this.traceId=traceId;
    }
}
