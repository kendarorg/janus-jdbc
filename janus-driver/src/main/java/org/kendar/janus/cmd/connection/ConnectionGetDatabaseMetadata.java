package org.kendar.janus.cmd.connection;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionGetDatabaseMetadata implements JdbcCommand {
    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var connection = (Connection)context.getConnection();
        return connection.getMetaData();
    }

    @Override
    public void serialize(TypedSerializer builder) {

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        return this;
    }

    @Override
    public String toString() {
        return "ConnectionGetDatabaseMetadata{}";
    }

    @Override
    public String getPath() {
        return "/Connection/getDatabaseMetadata";
    }
}
