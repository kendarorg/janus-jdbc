package org.kendar.janus.cmd.resultset;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultSetGetMetaData implements JdbcCommand{
    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var rs = (ResultSet)context.get(uid);
        return rs.getMetaData();
    }

    @Override
    public void serialize(TypedSerializer builder) {

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        return this;
    }
    @Override
    public String getPath() {
        return "/ResultSet/getMetaData";
    }
}
