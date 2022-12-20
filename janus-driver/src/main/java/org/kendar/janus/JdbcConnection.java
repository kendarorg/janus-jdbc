package org.kendar.janus;

import org.kendar.janus.cmd.Close;
import org.kendar.janus.cmd.Exec;
import org.kendar.janus.cmd.Exec;
import org.kendar.janus.cmd.connection.*;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.enums.ResultSetConcurrency;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.results.StatementResult;
import org.kendar.janus.server.JdbcContext;
import org.kendar.janus.types.*;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

public class JdbcConnection implements Connection {
    private long traceId;

    private Engine engine;
    private boolean closed = false;

    public JdbcConnection(long traceId, Engine engine) {
        this.traceId = traceId;
        this.engine = engine;
    }

    private JdbcStatement getJdbcStatement(ConnectionCreateStatement command) throws SQLException {
        var statement = (StatementResult)engine.execute(command,getTraceId(),getTraceId());
        return new JdbcStatement(this, engine,
                statement.getTraceId(),
                statement.getMaxRows(),
                statement.getQueryTimeout(),
                command.getType(),
                command.getConcurrency(),
                command.getHoldability());
    }
    @Override
    public Statement createStatement() throws SQLException {
        var command = new ConnectionCreateStatement();
        return getJdbcStatement(command);
    }


    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        var command = new ConnectionCreateStatement()
                .withType(ResultSetType.valueOf(resultSetType))
                .withConcurrency(ResultSetConcurrency.valueOf(resultSetConcurrency));
        return getJdbcStatement(command);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        var command = new ConnectionCreateStatement()
                .withType(ResultSetType.valueOf(resultSetType))
                .withConcurrency(ResultSetConcurrency.valueOf(resultSetConcurrency))
                .withHoldability(ResultSetHoldability.valueOf(resultSetHoldability));
        return getJdbcStatement(command);
    }



    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        var command = new ConnectionGetDatabaseMetadata();
        var metadata = (JdbcDatabaseMetaData)engine.execute(command,getTraceId(),getTraceId());
        metadata.initialize(this,engine);
        return metadata;
    }

    @Override
    public void close() throws SQLException {
        if (!this.isClosed()) {
            //FIXME REMOVE METADATA
            this.engine.execute(new Close(),this.getTraceId(),this.getTraceId());
            this.closed = true;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }



    private JdbcPreparedStatement getJdbcPreparedStatement(ConnectionPrepareStatement command) throws SQLException {
        var statement = (StatementResult)engine.execute(command,getTraceId(),getTraceId());
        return new JdbcPreparedStatement(this, engine,
                statement.getTraceId(),
                statement.getMaxRows(),
                statement.getQueryTimeout(),
                command.getType(),
                command.getConcurrency(),
                command.getHoldability())
                .withSql(command.getSql());
    }


    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        var command = new ConnectionPrepareStatement(sql);
        return getJdbcPreparedStatement(command);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        var command = new ConnectionPrepareStatement(sql)
                .withType(ResultSetType.valueOf(resultSetType))
                .withConcurrency(ResultSetConcurrency.valueOf(resultSetConcurrency));
        return getJdbcPreparedStatement(command);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        var command = new ConnectionPrepareStatement(sql)
                .withType(ResultSetType.valueOf(resultSetType))
                .withConcurrency(ResultSetConcurrency.valueOf(resultSetConcurrency))
                .withHoldability(ResultSetHoldability.valueOf(resultSetHoldability));
        return getJdbcPreparedStatement(command);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        var command = new ConnectionPrepareStatement(sql)
                .withAutoGeneratedKeys(autoGeneratedKeys);
        return getJdbcPreparedStatement(command);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        var command = new ConnectionPrepareStatement(sql)
                .withColumnIndexes(columnIndexes);
        return getJdbcPreparedStatement(command);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        var command = new ConnectionPrepareStatement(sql)
                .withColumnNames(columnNames);
        return getJdbcPreparedStatement(command);
    }




    @Override
    public Clob createClob() throws SQLException {
        return new JdbcClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return new JdbcBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return new JdbcNClob();
    }

    

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        var command = (ConnectionPrepareCall)new ConnectionPrepareCall()
                .withSql(sql);
        return getJdbcCallableStatement(command);
    }

    private JdbcCallableStatement getJdbcCallableStatement(ConnectionPrepareCall command) throws SQLException {
        var statement = (StatementResult)engine.execute(command,getTraceId(),getTraceId());
        return (JdbcCallableStatement)new JdbcCallableStatement(this, engine,
                statement.getTraceId(),
                statement.getMaxRows(),
                statement.getQueryTimeout(),
                command.getType(),
                command.getConcurrency(),
                command.getHoldability()
        )
                .withSql(command.getSql());

    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "nativeSQL")
                        .withTypes(String.class)
                        .withParameters(sql)
                        .onConnection()
                ,getTraceId(),getTraceId())).getResult();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        engine.execute(new Exec(
                        "setAutoCommit")
                        .withTypes(boolean.class)
                        .withParameters(autoCommit)
                        .onConnection()
                ,getTraceId(),getTraceId());
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getAutoCommit")
                        .onConnection()
                ,this.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public void commit() throws SQLException {
        engine.execute(new Exec(
                        "commit")
                        .onConnection()
                ,this.getTraceId(),getTraceId());
    }

    @Override
    public void rollback() throws SQLException {
        engine.execute(new Exec(
                        "rollback")
                        .onConnection()
                ,this.traceId,traceId);
    }



    @Override
    public void setHoldability(int holdability) throws SQLException {
        engine.execute(new Exec(
                        "setHoldability")
                        .withParameters(holdability)
                        .withTypes(int.class)
                        .onConnection()
                , this.traceId, traceId);
    }


    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        var command = (ConnectionPrepareCall) new ConnectionPrepareCall()
                .withSql(sql)
                .withType(ResultSetType.valueOf(resultSetType))
                .withConcurrency(ResultSetConcurrency.valueOf(resultSetConcurrency))
                .withHoldability(ResultSetHoldability.valueOf(resultSetHoldability));
        return getJdbcCallableStatement(command);
    }



    @Override
    public SQLXML createSQLXML() throws SQLException {
        return new JdbcSQLXML();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            engine.execute(new Exec(
                            "setClientInfo")
                            .withParameters(name,value)
                            .withTypes(String.class,String.class)
                            .onConnection()
                    , this.traceId, traceId);
        } catch (SQLException e) {
            throw new SQLClientInfoException();
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            engine.execute(new Exec(
                            "setClientInfo")
                            .withParameters(properties)
                            .withTypes(Properties.class)
                            .onConnection()
                    , this.traceId, traceId);
        } catch (SQLException e) {
            throw new SQLClientInfoException();
        }
    }

    @Override
    public Array createArrayOf( String typeName, final Object[] elements) throws SQLException {
        return new JdbcArray(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return new JdbcStruct().fromData(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        engine.execute(new Exec(
                        "setSchema")
                        .withParameters(schema)
                        .withTypes(String.class)
                        .onConnection()
                , this.traceId, traceId);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T)this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(JdbcConnection.class);
    }

    public long getTraceId() {
        return traceId;
    }

    public void setTraceId(long traceId) {
        this.traceId = traceId;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        engine.execute(new Exec(
                        "setReadOnly")
                        .withParameters(readOnly)
                        .withTypes(boolean.class)
                        .onConnection()
                ,this.traceId,traceId);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        engine.execute(new Exec(
                        "setCatalog")
                        .withParameters(catalog)
                        .withTypes(String.class)
                        .onConnection()
                , this.traceId, traceId);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        engine.execute(new Exec(
                        "setTransactionIsolation")
                        .withParameters(level)
                        .withTypes(int.class)
                        .onConnection()
                , this.traceId, traceId);
    }
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        var command = (ConnectionPrepareCall) new ConnectionPrepareCall()
                .withSql(sql)
                .withType(ResultSetType.valueOf(resultSetType))
                .withConcurrency(ResultSetConcurrency.valueOf(resultSetConcurrency));
        return getJdbcCallableStatement(command);
    }



    @Override
    public Savepoint setSavepoint() throws SQLException {
        return (JdbcSavepoint)engine.execute(new Exec(
                        "setSavepoint")
                        .onConnection()
                , this.traceId, traceId);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return (JdbcSavepoint)engine.execute(new Exec(
                        "setSavepoint")
                        .withTypes(String.class)
                        .withParameters(name)
                        .onConnection()
                , this.traceId, traceId);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        var jdbcSavepoint = (JdbcSavepoint)savepoint;
        var command = new ConnectionRollbackSavepoint(jdbcSavepoint.getTraceId());
        engine.execute(command,this.traceId,traceId);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        var jdbcSavepoint = (JdbcSavepoint)savepoint;
        var command = new ConnectionReleaseSavepoint(jdbcSavepoint.getTraceId());
        engine.execute(command,this.traceId,traceId);
    }


    @Override
    public boolean isReadOnly() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "isReadOnly")
                        .onConnection()
                , this.traceId, traceId)).getResult();
    }

    @Override
    public String getCatalog() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getCatalog")
                        .onConnection()
                , this.traceId, traceId)).getResult();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getTransactionIsolation")
                        .onConnection()
                , this.traceId, traceId)).getResult();
    }




    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getTypeMap")
                        .onConnection()
                , this.traceId, traceId)).getResult();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        engine.execute(new Exec(
                        "setTypeMap")
                        .withParameters(map)
                        .withTypes(Map.class)
                        .onConnection()
                , this.traceId, traceId);
    }

    @Override
    public int getHoldability() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getHoldability")
                        .onConnection()
                , this.traceId, traceId)).getResult();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "isValid")
                        .withParameters(timeout)
                        .withTypes(int.class)
                        .onConnection()
                , this.traceId, traceId)).getResult();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getClientInfo")
                        .withParameters(name)
                        .withTypes(String.class)
                        .onConnection()
                , this.traceId, traceId)).getResult();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getClientInfo")
                        .onConnection()
                , this.traceId, traceId)).getResult();
    }

    @Override
    public String getSchema() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getSchema")
                        .onConnection()
                , this.traceId, traceId)).getResult();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getNetworkTimeout")
                        .onConnection()
                , this.traceId, traceId)).getResult();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.close();
    }



    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        //TODO
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new UnsupportedOperationException("?setNetworkTimeout");
    }
}
