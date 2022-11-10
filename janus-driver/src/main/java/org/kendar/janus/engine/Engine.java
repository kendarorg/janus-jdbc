package org.kendar.janus.engine;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.results.JdbcResult;

import java.sql.SQLException;

public interface Engine {
    JdbcResult execute(JdbcCommand command, Long connectionId, Long uid) throws SQLException;

    int getMaxRows();

    boolean getPrefetchMetadata();

    String getCharset();
}
