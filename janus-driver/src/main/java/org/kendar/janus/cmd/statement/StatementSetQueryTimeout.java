package org.kendar.janus.cmd.statement;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;
import java.sql.Statement;

public class StatementSetQueryTimeout implements JdbcCommand {
    private int queryTimeout;

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public StatementSetQueryTimeout(){

    }

    public StatementSetQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var statement = (Statement)context.get(uid);
        statement.setQueryTimeout(queryTimeout);
        return null;
    }

    @Override
    public void serialize(TypedSerializer builder) {

        builder.write("queryTimeout", queryTimeout);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        queryTimeout = builder.read("queryTimeout");
        return this;
    }
}