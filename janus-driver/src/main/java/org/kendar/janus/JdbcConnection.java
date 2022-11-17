package org.kendar.janus;

import org.kendar.janus.cmd.Close;
import org.kendar.janus.cmd.Exec;
import org.kendar.janus.cmd.connection.ConnectionCreateStatement;
import org.kendar.janus.cmd.connection.ConnectionGetDatabaseMetadata;
import org.kendar.janus.cmd.connection.ConnectionPrepareStatement;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.enums.ResultSetConcurrency;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.results.StatementResult;
import org.kendar.janus.types.JdbcArray;
import org.kendar.janus.types.JdbcBlob;
import org.kendar.janus.types.JdbcClob;
import org.kendar.janus.types.JdbcNClob;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

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

    //TODO Implements

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        engine.execute(new Exec(
                        "setAutoCommit")
                        .withTypes(boolean.class)
                        .withParameters(autoCommit)
                ,getTraceId(),getTraceId());
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getAutoCommit")
                ,this.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public void commit() throws SQLException {
        engine.execute(new Exec(
                        "commit")
                ,this.getTraceId(),getTraceId());
    }

    @Override
    public void rollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException();
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
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }


    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }



    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array createArrayOf( String typeName, final Object[] elements) throws SQLException {
        return new JdbcArray(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSchema() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new UnsupportedOperationException();
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
}
