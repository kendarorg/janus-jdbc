package org.kendar.janus.cmd.preparedstatement;

import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PreparedStatementExecute extends PreparedStatementExecuteBase {

    private boolean loadRsOnExec;

    public PreparedStatementExecute(){

    }

    public PreparedStatementExecute(String sql, List<PreparedStatementParameter> parameters,boolean loadRsOnExec) {
        super(sql,parameters);
        this.loadRsOnExec = loadRsOnExec;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        super.serialize(builder);
        builder.write("loadRsOnExec",loadRsOnExec);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        super.deserialize(builder);
        loadRsOnExec =builder.read("loadRsOnExec");
        return this;
    }
    @Override
    protected Object executeInternal(PreparedStatement statement) throws SQLException {
        var result = statement.execute();

        if(!loadRsOnExec){
            return result;
        }
        if(result){
            return statement.getResultSet();
        }
        return false;
    }
    @Override
    public String getPath() {
        return "/PreparedStatement/execute";
    }
}
