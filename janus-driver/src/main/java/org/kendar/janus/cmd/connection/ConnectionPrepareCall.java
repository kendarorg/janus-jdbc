package org.kendar.janus.cmd.connection;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPrepareCall extends ConnectionPrepareStatement {

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var connection = (Connection) context.get(uid);
        if (type == null && concurrency == null) {
            return connection.prepareCall(sql);
        }
        if(!connection.getMetaData().supportsResultSetType(type.getValue())){
            throw new SQLException(type+" not supported");
        }
        if(!connection.getMetaData().supportsResultSetConcurrency(type.getValue(),concurrency.getValue())){
            throw new SQLException(type+":"+concurrency+" not supported");
        }

        if(holdability!= ResultSetHoldability.DEFAULT){
            if(!connection.getMetaData().supportsResultSetHoldability(holdability.getValue())){
                throw new SQLException(holdability+" not supported");
            }
            return connection.prepareCall (sql,type.getValue(), concurrency.getValue(),holdability.getValue());
        }

        return connection.prepareCall(sql, type.getValue(),concurrency.getValue());
    }
}
