package org.kendar.janus.cmd.preparedstatement;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PreparedStatementExecuteBatch implements JdbcCommand {
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
            for(var par:batch){
                par.load(statement);
            }
            statement.addBatch();
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
}
