package org.kendar.janus.cmd.preparedstatement;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PreparedStatementGetMetaData implements JdbcCommand{
    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var rs = (PreparedStatement)context.get(uid);
        return rs.getMetaData();
    }

    @Override
    public void serialize(TypedSerializer builder) {

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        return this;
    }
}
