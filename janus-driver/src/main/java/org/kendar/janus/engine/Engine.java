package org.kendar.janus.engine;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.results.JdbcResult;

import java.sql.SQLException;
import java.util.UUID;

public interface Engine {
    Engine create();

    JdbcResult execute(JdbcCommand command, Long connectionId, Long uid) throws SQLException;

    int getMaxRows();

    boolean getPrefetchMetadata();

    String getCharset();

    UUID startRecording();
    void cleanRecordings();
    void stopRecording(UUID id);
    void startReplaying(UUID id);
    void stopReplaying(UUID id);
}
