package org.kendar.janus;

import org.kendar.janus.cmd.Close;
import org.kendar.janus.cmd.Exec;
import org.kendar.janus.cmd.statement.*;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.enums.ResultSetConcurrency;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.utils.ExceptionsWrapper;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcStatement implements Statement {
    protected final ResultSetType resultSetType;
    protected ResultSetConcurrency concurrency;
    private ResultSetHoldability holdability;
    protected boolean closed=false;
    protected JdbcConnection connection;
    protected Engine engine;
    protected long traceId;
    protected int maxRows;
    private int queryTimeout;

    protected ResultSet resultSet;
    private boolean closeOnCompletion;

    public JdbcStatement(JdbcConnection connection, Engine engine, long traceId, int maxRows, int queryTimeout,
                         ResultSetType resultSetType, ResultSetConcurrency concurrency, ResultSetHoldability holdability) {

        this.connection = connection;
        this.engine = engine;
        this.traceId = traceId;
        this.maxRows = maxRows;
        this.queryTimeout = queryTimeout;
        this.resultSetType = resultSetType;
        this.concurrency = concurrency;
        this.holdability = holdability;
    }


    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }


    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return resultSetType.getValue();
    }

    @Override
    public boolean execute(final String sql) throws SQLException {
        this.resultSet = null;
        var command = new StatementExecute(sql,-1,null,null);
        var objectResult = (ObjectResult)engine.execute(command,connection.getTraceId(),getTraceId());
        return objectResult.getResult();
    }

    @Override
    public void close() throws SQLException {
        if (!this.isClosed()) {
            //FIXME CLOSE METADATA
            this.engine.execute(new Close(),connection.getTraceId(),this.getTraceId());
            this.closed = true;
        }
    }



    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        var result = (JdbcResultSet)engine.execute(new StatementGetGeneratedKeys(),this.connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        result.setStatement(this);
        return result;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            var command = new StatementExecuteQuery(
                    sql,
                    this.resultSetType
            );
            var result = (JdbcResultSet)this.engine.execute(command,connection.getTraceId(),getTraceId());
            result.setEngine(engine);
            result.setConnection(connection);
            result.setStatement(this);
            this.resultSet = result;
            return result;
        }
        catch (SQLException e) {
            throw e;
        }
        catch (Exception e2) {
            throw ExceptionsWrapper.toSQLException(e2);
        }
    }



    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T)this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(JdbcStatement.class);
    }

    public long getTraceId() {
        return traceId;
    }



    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {

        var command = new StatementExecute(sql,autoGeneratedKeys,null,null);
        return ((ObjectResult)engine.execute(command,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {

        var command = new StatementExecute(sql,-1,columnIndexes,null);
        return ((ObjectResult)engine.execute(command,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {


        var command = new StatementExecute(sql,-1,null,columnNames);
        return ((ObjectResult)engine.execute(command,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        var command = new StatementExecuteUpdate(sql,autoGeneratedKeys,null,null);
        return ((ObjectResult)engine.execute(command,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        var command = new StatementExecuteUpdate(sql,-1,columnIndexes,null);
        return ((ObjectResult)engine.execute(command,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        var command = new StatementExecuteUpdate(sql,-1,null,columnNames);
        return ((ObjectResult)engine.execute(command,connection.getTraceId(),getTraceId())).getResult();
    }



    @Override
    public int executeUpdate(String sql) throws SQLException {
        var command = new StatementExecuteUpdate(sql,-1,null,null);
        return ((ObjectResult)engine.execute(command,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if(resultSet==null){
            var command = new StatementGetResultSet();
            var result = (JdbcResultSet)this.engine.execute(command,connection.getTraceId(),getTraceId());
            result.setEngine(engine);
            result.setConnection(connection);
            result.setStatement(this);
            this.resultSet = result;
        }
        return resultSet;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        var command = new StatementSetMaxRows(max);
        this.engine.execute(command,connection.getTraceId(),getTraceId());
        this.maxRows = max;
    }


    @Override
    public int getMaxFieldSize() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxFieldSize")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        ((ObjectResult)engine.execute(new Exec(
                        "setMaxFieldSize")
                        .withTypes(int.class)
                        .withParameters(max)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        ((ObjectResult)engine.execute(new Exec(
                        "setEscapeProcessing")
                        .withTypes(boolean.class)
                        .withParameters(enable)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        var command = new StatementSetQueryTimeout(seconds);
        this.engine.execute(command,connection.getTraceId(),getTraceId());
        this.queryTimeout = seconds;
    }



    @Override
    public int getUpdateCount() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getUpdateCount")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMoreResults")
                ,connection.getTraceId(),getTraceId())).getResult();
    }


    @Override
    public void setFetchSize(int rows) throws SQLException {
        ((ObjectResult)engine.execute(new Exec(
                        "setFetchSize")
                        .withTypes(int.class)
                        .withParameters(rows)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getFetchSize() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getFetchSize")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return concurrency.getValue();
    }


    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMoreResults")
                        .withTypes(int.class)
                        .withParameters(current)
                ,connection.getTraceId(),getTraceId())).getResult();
    }


    @Override
    public void cancel() throws SQLException {
        engine.execute(new Exec(
                        "cancel")
                ,connection.getTraceId(),getTraceId());
        if(resultSet!=null) {
            resultSet.close();
        }
    }



    @Override
    public void setFetchDirection(int direction) throws SQLException {
        engine.execute(new Exec(
                        "setFetchDirection")
                        .withTypes(int.class)
                        .withParameters(direction)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getFetchDirection")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return this.holdability.getValue();
    }


    private List<String> batches = new ArrayList<>();
    @Override
    public void addBatch(String sql) throws SQLException {
        batches.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        batches=new ArrayList<>();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new StatementExecuteBatch(batches),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }




    @Override
    public void closeOnCompletion() throws SQLException {
        engine.execute(new Exec(
                        "closeOnCompletion")
                ,connection.getTraceId(),getTraceId());
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return closeOnCompletion;
    }


    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        engine.execute(new Exec(
                        "setPoolable")
                        .withTypes(boolean.class)
                        .withParameters(poolable)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "isPoolable")
                ,connection.getTraceId(),getTraceId())).getResult();
    }



    @Override
    public void setCursorName(String name) throws SQLException {
        engine.execute(new Exec(
                        "setCursorName")
                        .withTypes(String.class)
                        .withParameters(name)
                ,connection.getTraceId(),getTraceId());
    }

    

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }


    @Override
    public long executeLargeUpdate(String sql, int columnIndexes[]) throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new StatementExecuteLargeUpdate(batches,sql,columnIndexes),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }
    @Override
    public long executeLargeUpdate(String sql, String columnNames[])
            throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new StatementExecuteLargeUpdate(batches,sql,columnNames),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }
    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new StatementExecuteLargeUpdate(batches,sql,autoGeneratedKeys),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new StatementExecuteLargeUpdate(batches,sql),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }


    @Override
    public long[] executeLargeBatch() throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new StatementExecuteLargeBatch(batches),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }
}
