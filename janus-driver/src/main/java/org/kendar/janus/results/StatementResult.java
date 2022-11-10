package org.kendar.janus.results;

import org.kendar.janus.serialization.TypedSerializer;

public class StatementResult implements JdbcResult {
    private long traceId;
    private int maxRows;
    private int queryTimeout;

    public int getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public StatementResult(){

    }
    public StatementResult(Long traceId, int maxRows, int queryTimeout) {

        this.traceId = traceId;
        this.maxRows = maxRows;
        this.queryTimeout = queryTimeout;
    }


    public long getTraceId() {
        return traceId;
    }

    public void setTraceId(long traceId) {
        this.traceId = traceId;
    }
    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("traceId",traceId);
        builder.write("maxRows",maxRows);
        builder.write("queryTimeout",queryTimeout);
    }

    @Override
    public JdbcResult deserialize(TypedSerializer input) {
        return
                new StatementResult(
                        (Long) input.read("traceId"),
                        (Integer) input.read("maxRows"),
                        (Integer) input.read("queryTimeout")
                );
    }
}
