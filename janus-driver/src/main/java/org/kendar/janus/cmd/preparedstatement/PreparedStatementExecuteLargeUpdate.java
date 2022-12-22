package org.kendar.janus.cmd.preparedstatement;

import jdk.jshell.spi.ExecutionControl;
import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import javax.naming.OperationNotSupportedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class PreparedStatementExecuteLargeUpdate implements JdbcCommand {
    private int[] columnIndexes;
    private  String[] columnNames;
    private List<List<PreparedStatementParameter>> batches;
    private String sql;
    private Integer autoGeneratedKeys;

    public PreparedStatementExecuteLargeUpdate(){

    }

    public PreparedStatementExecuteLargeUpdate(List<List<PreparedStatementParameter>> batches, String sql) {

        this.batches = batches;
        this.sql = sql;
    }

    public PreparedStatementExecuteLargeUpdate(List<List<PreparedStatementParameter>> batches, String sql, int autoGeneratedKeys) {

        this.batches = batches;
        this.sql = sql;
        this.autoGeneratedKeys = autoGeneratedKeys;
    }

    public PreparedStatementExecuteLargeUpdate(List<List<PreparedStatementParameter>> batches, String sql, String[] columnNames) {
        this.batches = batches;
        this.sql = sql;
        this.columnNames = columnNames;
    }

    public PreparedStatementExecuteLargeUpdate(List<List<PreparedStatementParameter>> batches, String sql, int[] columnIndexes) {
        this.batches = batches;
        this.sql = sql;
        this.columnIndexes = columnIndexes;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var statement = (PreparedStatement)context.get(uid);
        for(var batch:batches){
            if(batch.isEmpty()){
                statement.addBatch();
            }else{
                for(var par:batch){
                    par.load(statement);
                }
                statement.addBatch();
            }
        }
        if(autoGeneratedKeys!=null){
            return statement.executeLargeUpdate(sql,autoGeneratedKeys);
        }
        if(columnNames!=null){
            return statement.executeLargeUpdate(sql,columnNames);
        }
        if(columnIndexes!=null){
            return statement.executeLargeUpdate(sql,columnIndexes);
        }
        return statement.executeLargeUpdate(sql);
    }

    @Override
    public void serialize(TypedSerializer builder) {

        builder.write("batches",batches);
        builder.write("autoGeneratedKeys",autoGeneratedKeys);
        builder.write("columnNames",columnNames);
        builder.write("columnIndexes",columnIndexes);
        builder.write("sql",sql);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        batches = builder.read("batches");
        autoGeneratedKeys = builder.read("autoGeneratedKeys");
        columnNames = builder.read("columnNames");
        columnIndexes = builder.read("columnIndexes");
        sql = builder.read("sql");
        return this;
    }

    @Override
    public String toString() {
        return "StatementExecuteLargeUpdate{" +
                "batches=" + batches +
                ",sql=" + sql +
                '}';
    }
    @Override
    public String getPath() {
        return "/PreparedStatement/executeLargeUpdate";
    }
}
