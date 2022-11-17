package org.kendar.janus.cmd.preparedstatement;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public abstract class PreparedStatementExecuteBase implements JdbcCommand {
    protected String sql;
    protected List<PreparedStatementParameter> parameters;

    public PreparedStatementExecuteBase(){

    }

    public PreparedStatementExecuteBase(String sql, List<PreparedStatementParameter> parameters) {

        this.sql = sql;
        this.parameters = parameters;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var statement = (PreparedStatement)context.get(uid);
        for (int i = 0; i < parameters.size(); i++) {
            PreparedStatementParameter param = parameters.get(i);
            if(param==null) continue;
            param.load(statement);

        }
        return executeInternal(statement);
    }

    protected abstract Object executeInternal(PreparedStatement statement) throws SQLException;

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("sql",sql);
        builder.write("parameters",parameters);

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer input) {
        sql = input.read("sql");
        parameters = input.read("parameters");
        return this;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+"{" +
                "\n\tsql='" + sql + '\'' +
                ", \n\tparameters=" + parameters +
                '}';
    }
}
