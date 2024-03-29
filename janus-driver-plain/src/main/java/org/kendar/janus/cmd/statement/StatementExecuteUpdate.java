package org.kendar.janus.cmd.statement;

import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.cmd.interfaces.JdbcSqlCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class StatementExecuteUpdate implements JdbcCommand, JdbcSqlCommand {
    private String sql;
    private Integer autoGeneratedKeys = null;
    private int[] columnIndexes;
    private String[] columnNames;

    public StatementExecuteUpdate(){

    }

    public StatementExecuteUpdate(String sql, int autoGeneratedKeys, int[] columnIndexes, String[] columnNames) {
        this.sql = sql;
        this.autoGeneratedKeys = autoGeneratedKeys==-1?null:autoGeneratedKeys;
        this.columnIndexes = columnIndexes;
        this.columnNames = columnNames;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        //FIXME TO TEST
        var statement = (Statement)context.get(uid);
        if(autoGeneratedKeys != null){
            return statement.executeUpdate(sql,autoGeneratedKeys);
        }else if(columnIndexes!=null){
            return statement.executeUpdate(sql,columnIndexes);
        }else if(columnNames!=null){
            return statement.executeUpdate(sql,columnNames);
        }
        return statement.executeUpdate(sql);
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("sql",sql);
        builder.write("autoGeneratedKeys",autoGeneratedKeys);
        builder.write("columnIndexes",columnIndexes);
        builder.write("columnNames",columnNames);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {

        sql =builder.read("sql");
        autoGeneratedKeys =builder.read("autoGeneratedKeys");
        columnIndexes =builder.read("columnIndexes");
        columnNames =builder.read("columnNames");
        return this;
    }

    @Override
    public String toString() {
        return "StatementExecuteUpdate{" +
                "\n\tsql='" + sql + '\'' +
                ", \n\tautoGeneratedKeys=" + autoGeneratedKeys +
                ", \n\tcolumnIndexes=" + Arrays.toString(columnIndexes) +
                ", \n\tcolumnNames=" + Arrays.toString(columnNames) +
                '}';
    }
    @Override
    public String getPath() {
        return "/Statement/executeUpdate";
    }

    @Override
    public String getSql() {
        return sql;
    }
}
