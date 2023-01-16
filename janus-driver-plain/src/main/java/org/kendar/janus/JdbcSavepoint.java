package org.kendar.janus;

import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.SQLException;
import java.sql.Savepoint;

public class JdbcSavepoint implements Savepoint, JdbcResult {
    public long getTraceId() {
        return traceId;
    }

    public void setTraceId(long traceId) {
        this.traceId = traceId;
    }

    private long traceId;
    public int getSavePointId() {
        return savePointId;
    }

    public String getSavePointName() {
        return savePointName;
    }

    public void setSavePointName(String savePointName) {
        this.savePointName = savePointName;
    }

    public void setSavePointId(int savePointId) {
        this.savePointId = savePointId;
    }

    private int savePointId;
    private String savePointName;
    @Override
    public int getSavepointId() throws SQLException {
        return savePointId;
    }

    @Override
    public String getSavepointName() throws SQLException {
        return savePointName;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("savePointId",savePointId);
        builder.write("savePointName",savePointName);
        builder.write("traceId",traceId);
    }

    @Override
    public JdbcResult deserialize(TypedSerializer builder) {
        savePointId = builder.read("savePointId");
        savePointName = builder.read("savePointName");
        traceId = builder.read("traceId");
        return this;
    }
}
