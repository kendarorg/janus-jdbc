package org.kendar.janus.cmd.connection;

import org.kendar.janus.JdbcConnection;
import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

public class ConnectionRollbackSavepoint implements JdbcCommand {

    private long traceId;

    public ConnectionRollbackSavepoint(){

    }
    public ConnectionRollbackSavepoint(long traceId){

        this.traceId = traceId;
    }
    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var connection = (Connection)context.get(uid);
        var savePoint = (Savepoint)context.get(traceId);
        connection.rollback(savePoint);
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
}
