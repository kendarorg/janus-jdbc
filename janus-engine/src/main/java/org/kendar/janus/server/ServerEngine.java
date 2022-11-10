package org.kendar.janus.server;

import org.kendar.janus.cmd.Close;
import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.cmd.connection.ConnectionConnect;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.utils.JdbcTypesConverter;
import org.kendar.janus.utils.LoggerWrapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ServerEngine implements Engine {
    static ConcurrentHashMap<Long, JdbcContext> contexts = new ConcurrentHashMap<>();
    static LoggerWrapper log = LoggerWrapper.getLogger(ServerEngine.class);
    private int maxRows;

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    public boolean isPrefetchMetadata() {
        return prefetchMetadata;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public void setPrefetchMetadata(boolean prefetchMetadata) {
        this.prefetchMetadata = prefetchMetadata;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    private boolean prefetchMetadata;
    private String charset;
    private int queryTimeout;
    private String connectionString;
    private String login;
    private String password;
    private final AtomicLong traceId = new AtomicLong();

    private long getTraceId(){
        return traceId.incrementAndGet();
    }

    public ServerEngine(String connectionString, String login, String password) {

        this.maxRows = 3;
        this.prefetchMetadata = false;
        this.charset = "UTF-8";
        this.queryTimeout = 0;
        this.connectionString = connectionString;
        this.login = login;
        this.password = password;
    }

    @Override
    public JdbcResult execute(JdbcCommand command, Long connectionId, Long uid) throws SQLException {
        JdbcResult result = null;
        Object resultObject = null;
        Long traceId = null;
        if(command instanceof ConnectionConnect){
            resultObject = handleConnect((ConnectionConnect)command);
        }else if(command instanceof Close){
            resultObject = handleClose((Close)command,connectionId,uid);
        }else{
            resultObject = handle(command,connectionId,uid);
            traceId = getTraceId();
            contexts.get(connectionId).put(traceId,resultObject);
        }
        return JdbcTypesConverter.convertResult(this,resultObject,connectionId,traceId);
    }

    @Override
    public int getMaxRows() {
        return maxRows;
    }

    @Override
    public boolean getPrefetchMetadata() {
        return prefetchMetadata;
    }

    @Override
    public String getCharset() {
        return charset;
    }


    private Object handle(JdbcCommand command, Long connectionId, Long uid) throws SQLException {

        var result = command.execute(contexts.get(connectionId),uid);
        return result;
    }

    private JdbcResult handleClose(Close command, Long connectionId, Long uid) throws SQLException {
        command.execute(contexts.get(connectionId),uid);
        //FIXME Close everything
        contexts.remove(traceId);
        return null;
    }

    private Long handleConnect(ConnectionConnect command) throws SQLException {
        log.debug("Connected to "+command.getDatabase());
        if(command.getProperties()!=null) {
            for (var prop : command.getProperties().entrySet()) {
                log.debug("Setting Property " + prop.getKey() + ":" + prop.getValue());
            }
        }
        if(command.getClientInfo()!=null) {
            for (var prop : command.getClientInfo().entrySet()) {
                log.debug("Setting ClientInfo " + prop.getKey() + ":" + prop.getValue());
            }
        }
        Connection conn = DriverManager.
                getConnection(connectionString, login, password);

        var traceId = getTraceId();
        contexts.put(traceId,new JdbcContext());
        contexts.get(traceId).put(traceId,conn);

        return traceId;
    }
}
