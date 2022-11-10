package org.kendar.janus.cmd.statement;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;
import java.sql.Statement;

public class StatementExecuteQuery implements JdbcCommand {
    private String sql;
    private ResultSetType resultSetType;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public StatementExecuteQuery(){

    }

    public StatementExecuteQuery(String sql, ResultSetType resultSetType) {
        this.sql = sql;
        this.resultSetType = resultSetType;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var statement = (Statement)context.get(uid);
        return statement.executeQuery(sql);
    }

    @Override
    public void serialize(TypedSerializer builder) {

        builder.write("sql",sql);
        builder.write("resultSetType",resultSetType);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {

        sql = builder.read("sql");
        resultSetType = builder.read("resultSetType");
        return this;
    }

    public ResultSetType getResultSetType() {
        return resultSetType;
    }

    public void setResultSetType(ResultSetType resultSetType) {
        this.resultSetType = resultSetType;
    }
}
