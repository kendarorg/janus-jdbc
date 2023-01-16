package org.kendar.janus.cmd.statement;

import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.cmd.interfaces.JdbcSqlBatches;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class StatementExecuteLargeBatch implements JdbcCommand, JdbcSqlBatches {
    private List<String> batches;

    public StatementExecuteLargeBatch(){

    }

    public StatementExecuteLargeBatch(List<String> batches) {

        this.batches = batches;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var statement = (Statement)context.get(uid);
        for(var batch:batches){
            statement.addBatch(batch);
        }
        return statement.executeLargeBatch();
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("batches",batches);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        batches = builder.read("batches");
        return this;
    }

    @Override
    public String toString() {
        return "StatementExecuteBatch{" +
                "batches=" + batches +
                '}';
    }
    @Override
    public String getPath() {
        return "/Statement/executeLargeBatch";
    }

    @Override
    public List<String> getBatches() {
        return batches;
    }
}
