package org.kendar.janus;

public interface TraceAwareType {
    long getTraceId();
    void setTraceId(long traceId);
}
