package org.kendar.janus.cmd.statement;

import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.cmd.interfaces.JdbcSqlCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class StatementExecute implements JdbcCommand, JdbcSqlCommand {
    private String sql;
    private Integer autoGeneratedKeys = null;
    private int[] columnIndexes;
    private String[] columnNames;
    private boolean loadRsOnExec;

    public StatementExecute(){

    }

    public StatementExecute(String sql, int autoGeneratedKeys, int[] columnIndexes, String[] columnNames,boolean loadRsOnExec) {
        this.sql = sql;
        this.autoGeneratedKeys = autoGeneratedKeys==-1?null:autoGeneratedKeys;
        this.columnIndexes = columnIndexes;
        this.columnNames = columnNames;
        this.loadRsOnExec = loadRsOnExec;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        //FIXME TO TEST
        var statement = (Statement)context.get(uid);
        var result = false;
        if(autoGeneratedKeys != null){
            result = statement.execute(sql,autoGeneratedKeys);
        }else if(columnIndexes!=null){
            result = statement.execute(sql,columnIndexes);
        }else if(columnNames!=null){
            result = statement.execute(sql,columnNames);
        }else {
            result = statement.execute(sql);
        }
        if(!loadRsOnExec){
            return result;
        }
        if(result){
            return statement.getResultSet();
        }
        return false;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("sql",sql);
        builder.write("autoGeneratedKeys",autoGeneratedKeys);
        builder.write("columnIndexes",columnIndexes);
        builder.write("columnNames",columnNames);
        builder.write("loadRsOnExec",loadRsOnExec);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {

        sql =builder.read("sql");
        autoGeneratedKeys =builder.read("autoGeneratedKeys");
        columnIndexes =builder.read("columnIndexes");
        columnNames =builder.read("columnNames");
        loadRsOnExec =builder.read("loadRsOnExec");
        return this;
    }

    @Override
    public String toString() {
        return "StatementExecute{" +
                "\n\tsql='" + sql + '\'' +
                ", \n\tautoGeneratedKeys=" + autoGeneratedKeys +
                ", \n\tcolumnIndexes=" + Arrays.toString(columnIndexes) +
                ", \n\tcolumnNames=" + Arrays.toString(columnNames) +
                '}';
    }
    @Override
    public String getPath() {
        return "/Statement/execute";
    }

    @Override
    public String getSql() {
        return sql;
    }
}
