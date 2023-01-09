package org.kendar.janus.cmd.statement;

import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;
import java.sql.Statement;

public class StatementSetMaxRows implements JdbcCommand {
    private int maxRows;

    public int getMaxRows() {
        return maxRows;
    }

    public StatementSetMaxRows(){

    }

    public StatementSetMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var statement = (Statement)context.get(uid);
        statement.setMaxRows(maxRows);
        return Void.TYPE;
    }

    @Override
    public void serialize(TypedSerializer builder) {

        builder.write("maxRows",maxRows);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        maxRows = builder.read("maxRows");
        return this;
    }

    @Override
    public String toString() {
        return "StatementSetMaxRows{" +
                "maxRows=" + maxRows +
                '}';
    }
    @Override
    public String getPath() {
        return "/Statement/setMaxRows";
    }
}
