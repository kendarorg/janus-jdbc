package org.kendar.janus.cmd.preparedstatement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PreparedStatementExecuteUpdate extends PreparedStatementExecuteBase {

    public PreparedStatementExecuteUpdate(){

    }

    public PreparedStatementExecuteUpdate(String sql, List<PreparedStatementParameter> parameters) {
        super(sql,parameters);
    }

    @Override
    protected Object executeInternal(PreparedStatement statement) throws SQLException {
        return statement.executeUpdate();
    }
    @Override
    public String getPath() {
        return "/PreparedStatement/executeUpdate";
    }
}
