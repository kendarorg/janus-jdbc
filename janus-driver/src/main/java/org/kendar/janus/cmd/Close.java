package org.kendar.janus.cmd;

import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;
import org.kendar.janus.utils.ExceptionsWrapper;

import java.sql.SQLException;

public class Close implements JdbcCommand {

    public Close(){

    }


    @Override
    public void serialize(TypedSerializer builder) {

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer input) {
        return new Close();
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        try {
            var jdbcObject = context.get(uid);
            var method = jdbcObject.getClass().getMethod("close");
            method.invoke(jdbcObject);
        }catch (Exception ex){
            ExceptionsWrapper.toSQLException(ex);
        }
        return null;
    }
}
