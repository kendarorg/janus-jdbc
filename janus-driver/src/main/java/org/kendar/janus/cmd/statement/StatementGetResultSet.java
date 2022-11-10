package org.kendar.janus.cmd.statement;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;
import java.sql.Statement;

public class StatementGetResultSet implements JdbcCommand {
    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var statement = (Statement)context.get(uid);
        return statement.getResultSet();
    }

    @Override
    public void serialize(TypedSerializer builder) {

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        return this;
    }
}
