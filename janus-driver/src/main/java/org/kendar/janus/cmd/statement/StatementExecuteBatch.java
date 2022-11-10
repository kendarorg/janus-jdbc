package org.kendar.janus.cmd.statement;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class StatementExecuteBatch implements JdbcCommand {
    private List<String> batches;

    public StatementExecuteBatch(){

    }

    public StatementExecuteBatch(List<String> batches) {

        this.batches = batches;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var statement = (Statement)context.get(uid);
        for(var batch:batches){
            statement.addBatch(batch);
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
