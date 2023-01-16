package org.kendar.janus.cmd.preparedstatement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PreparedStatementExecute extends PreparedStatementExecuteBase {

    public PreparedStatementExecute(){

    }

    public PreparedStatementExecute(String sql, List<PreparedStatementParameter> parameters) {
        super(sql,parameters);
    }

    @Override
    protected Object executeInternal(PreparedStatement statement) throws SQLException {
        return statement.execute();
    }
    @Override
    public String getPath() {
        return "/PreparedStatement/execute";
    }
}
