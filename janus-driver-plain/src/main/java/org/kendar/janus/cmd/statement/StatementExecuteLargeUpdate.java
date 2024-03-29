package org.kendar.janus.cmd.statement;

import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.cmd.interfaces.JdbcSqlBatches;
import org.kendar.janus.cmd.interfaces.JdbcSqlCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class StatementExecuteLargeUpdate implements JdbcCommand, JdbcSqlCommand, JdbcSqlBatches {
    private int[] columnIndexes;
    private  String[] columnNames;
    private List<String> batches;
    private String sql;
    private Integer autoGeneratedKeys;

    public StatementExecuteLargeUpdate(){

    }

    public StatementExecuteLargeUpdate(List<String> batches,String sql) {

        this.batches = batches;
        this.sql = sql;
    }

    public StatementExecuteLargeUpdate(List<String> batches, String sql, int autoGeneratedKeys) {

        this.batches = batches;
        this.sql = sql;
        this.autoGeneratedKeys = autoGeneratedKeys;
    }

    public StatementExecuteLargeUpdate(List<String> batches, String sql, String[] columnNames) {
        this.batches = batches;
        this.sql = sql;
        this.columnNames = columnNames;
    }

    public StatementExecuteLargeUpdate(List<String> batches, String sql, int[] columnIndexes) {
        this.batches = batches;
        this.sql = sql;
        this.columnIndexes = columnIndexes;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var statement = (Statement)context.get(uid);
        for(var batch:batches){
            statement.addBatch(batch);
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
        return "/Statement/executeLargeUpdate";
    }

    @Override
    public String getSql() {
        return sql;
    }

    @Override
    public List<String> getBatches() {
        return batches;
    }
}
