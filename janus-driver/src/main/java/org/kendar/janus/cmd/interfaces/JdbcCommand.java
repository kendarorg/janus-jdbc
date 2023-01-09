package org.kendar.janus.cmd.interfaces;

import org.kendar.janus.serialization.TypedSerializable;
import org.kendar.janus.server.JdbcContext;

import java.sql.SQLException;

public interface JdbcCommand extends TypedSerializable<JdbcCommand> {
    Object execute(JdbcContext context, Long uid) throws SQLException;
    String getPath();
}
