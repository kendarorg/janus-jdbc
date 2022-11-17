package org.kendar.janus.cmd.statement;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;
import java.sql.Statement;

public class StatementGetGeneratedKeys implements JdbcCommand {
    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var resultSet = ((Statement)context.get(uid)).getGeneratedKeys();
        return resultSet;
    }

    @Override
    public void serialize(TypedSerializer builder) {

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer input) {
        return this;
    }


    @Override
    public String toString() {
        return "StatementGetGeneratedKeys{}";
    }
}
