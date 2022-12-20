package org.kendar.janus.server;

import org.apache.commons.lang3.ClassUtils;
import org.kendar.janus.JdbcConnection;
import org.kendar.janus.cmd.Close;
import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.cmd.connection.ConnectionConnect;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.utils.JdbcTypesConverter;
import org.kendar.janus.utils.LoggerWrapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
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
    public Engine create() {
        return new ServerEngine(connectionString,login,password);
    }

    @Override
    public JdbcResult execute(JdbcCommand command, Long connectionId, Long uid) throws SQLException {
        try {
            if (isReplaying) {
                var recording = recordings.get(currentRecording);
                var result = recording.get((int) replayIndex);
                replayIndex++;
                return (JdbcResult) result.getResponse();
            }
            JdbcResult result = null;
            Object resultObject = null;
            Long traceId = null;
            if (command instanceof ConnectionConnect) {
                resultObject = handleConnect((ConnectionConnect) command);
                result = JdbcTypesConverter.convertResult(this, resultObject, connectionId, traceId);
            } else if (command instanceof Close) {
                resultObject = handleClose((Close) command, connectionId, uid);
                result = JdbcTypesConverter.convertResult(this, resultObject, connectionId, traceId);
            } else {
                resultObject = handle(command, connectionId, uid);
                var context = contexts.get(connectionId);
                traceId = context.getNext();
                result = JdbcTypesConverter.convertResult(this, resultObject, connectionId, traceId);
                if(resultObject!=null && !ClassUtils.isAssignable(result.getClass(), ObjectResult.class)) {
                    contexts.get(connectionId).put(traceId, resultObject);
                }
            }

            if (isRecording) {
                var recording = recordings.get(currentRecording);
                var rr = new ResponseRequest();
                var jdbcRequest = new JdbcRequest();
                jdbcRequest.setCommand(command);
                jdbcRequest.setConnectionId(connectionId);
                jdbcRequest.setConnectionId(uid);
                rr.setRequest(jdbcRequest);
                rr.setResponse(result);
                recording.add(rr);
            }
            return result;
        }catch(SQLException ex){
            int maxDepth = 10;

            SQLException founded = ex;
            Throwable current = ex;
            while(maxDepth>0){
                if(current.getCause()==null){
                    break;
                }
                if(ClassUtils.isAssignable(current.getCause().getClass(),SQLException.class)){
                    founded=(SQLException)current.getCause();
                    break;
                }
                current = current.getCause();
                maxDepth--;
            }
            throw founded;
        }
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

    private HashMap<UUID,List<ResponseRequest>> recordings = new HashMap<>();
    private boolean isRecording =false;
    private boolean isReplaying =false;
    private UUID currentRecording = null;
    private long replayIndex = 0;
    @Override
    public UUID startRecording() {
        var id = UUID.randomUUID();
        recordings.put(id,new ArrayList<>());
        currentRecording = id;
        isRecording = true;
        return id;
    }

    @Override
    public void stopRecording(UUID id) {
        isRecording = false;
        currentRecording = null;
    }

    @Override
    public void startReplaying(UUID id) {
        currentRecording = id;
        isReplaying = true;
        replayIndex =0;
    }

    @Override
    public void stopReplaying(UUID id) {
        currentRecording = id;
        isReplaying = false;
    }

    @Override
    public void cleanRecordings(){
        recordings.clear();
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
        contexts.get(traceId).setConnection(conn);

        return traceId;
    }
}
