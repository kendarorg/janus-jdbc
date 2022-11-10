package org.kendar.janus.engine;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.results.JdbcResult;

public class DriverEngine implements Engine {
    public JdbcResult execute(JdbcCommand command, Long connectionId, Long uid){
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxRows() {
        return 0;
    }

    @Override
    public boolean getPrefetchMetadata() {
        return false;
    }

    @Override
    public String getCharset() {
        return null;
    }
}
