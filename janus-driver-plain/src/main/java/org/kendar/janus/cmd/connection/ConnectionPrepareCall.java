package org.kendar.janus.cmd.connection;

import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.server.JdbcContext;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPrepareCall extends ConnectionPrepareStatement {

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var connection = (Connection) context.getConnection();
        if (type == null && concurrency == null) {
            return connection.prepareCall(sql);
        }

        if(holdability!= ResultSetHoldability.DEFAULT){

            return connection.prepareCall (sql,type.getValue(), concurrency.getValue(),holdability.getValue());
        }

        return connection.prepareCall(sql, type.getValue(),concurrency.getValue());
    }
    @Override
    public String getPath() {
        return "/Connection/prepareCall";
    }
}
