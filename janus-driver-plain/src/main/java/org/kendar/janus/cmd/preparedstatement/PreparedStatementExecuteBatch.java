package org.kendar.janus.cmd.preparedstatement;

import org.kendar.janus.cmd.interfaces.JdbcBatchPreparedStatementParameters;
import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PreparedStatementExecuteBatch implements JdbcCommand, JdbcBatchPreparedStatementParameters {
    private List<List<PreparedStatementParameter>> batches;

    public PreparedStatementExecuteBatch(){

    }

    public PreparedStatementExecuteBatch(List<List<PreparedStatementParameter>> batches) {

        this.batches = batches;
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
        return statement.executeBatch();
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
        return "PreparedStatementExecuteBatch{" +
                "batches=" + batches +
                '}';
    }
    @Override
    public String getPath() {
        return "/PreparedStatement/executeBatch";
    }

    @Override
    public List<List<PreparedStatementParameter>> getBatches() {
        return batches;
    }
}
