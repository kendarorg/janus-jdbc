package org.kendar.janus.engine;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.results.JdbcResult;

import java.util.UUID;

public class DriverEngine implements Engine {
    public JdbcResult execute(JdbcCommand command, Long connectionId, Long uid){
        throw new UnsupportedOperationException("?execute");
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

    @Override
    public UUID startRecording() {
        return null;
    }

    @Override
    public void cleanRecordings() {

    }

    @Override
    public void stopRecording(UUID id) {

    }

    @Override
    public void startReplaying(UUID id) {

    }

    @Override
    public void stopReplaying(UUID id) {

    }
}
