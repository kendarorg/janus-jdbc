package org.kendar.janus.cmd.preparedstatement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PreparedStatementExecuteQuery extends PreparedStatementExecuteBase {

    public PreparedStatementExecuteQuery(){

    }

    public PreparedStatementExecuteQuery(String sql, List<PreparedStatementParameter> parameters) {
        super(sql,parameters);
    }

    @Override
    protected Object executeInternal(PreparedStatement statement) throws SQLException {
        return statement.executeQuery();
    }
    @Override
    public String getPath() {
        return "/PreparedStatement/executeQuery";
    }
}
