package org.kendar.janus;

import org.kendar.janus.cmd.preparedstatement.*;
import org.kendar.janus.cmd.preparedstatement.parameters.*;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.enums.ResultSetConcurrency;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.types.JdbcBlob;
import org.kendar.janus.types.JdbcClob;
import org.kendar.janus.types.JdbcNClob;
import org.kendar.janus.types.JdbcSQLXML;
import org.kendar.janus.utils.ExceptionsWrapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {
    private String sql;
    private ResultSetMetaData metadata;

    public JdbcPreparedStatement(JdbcConnection connection, Engine engine, long traceId, int maxRows, int queryTimeout,
                                 ResultSetType resultSetType, ResultSetConcurrency concurrency, ResultSetHoldability holdability) {
        super(connection, engine, traceId, maxRows, queryTimeout, resultSetType, concurrency, holdability);
        parameters = new ArrayList<>();
    }

    protected List<PreparedStatementParameter> parameters;



    public JdbcPreparedStatement withSql(String sql) {
        this.sql = sql;
        return this;
    }



    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.setParameter(parameterIndex, new NullParameter(parameterIndex,sqlType,null));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.setParameter(parameterIndex, new NullParameter(parameterIndex,sqlType,typeName));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.setParameter(parameterIndex, new StringParameter(x,parameterIndex));
    }

    private void setParameter(int parameterIndex, PreparedStatementParameter parameter) {
        while(parameters.size()<parameterIndex){
            parameters.add(null);
        }
        parameters.set(parameterIndex-1,parameter);
    }

    @Override
    public int executeUpdate() throws SQLException {
        var result = (ObjectResult)this.engine.execute(
                new PreparedStatementExecuteUpdate(this.sql, this.parameters),
                this.connection.getTraceId(),
                getTraceId());
        return result.getResult();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.setParameter(parameterIndex, new BooleanParameter(x,parameterIndex));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.setParameter(parameterIndex, new ByteParameter(x,parameterIndex));

    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.setParameter(parameterIndex, new ShortParameter(x,parameterIndex));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.setParameter(parameterIndex, new IntParameter(x,parameterIndex));

    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.setParameter(parameterIndex, new LongParameter(x,parameterIndex));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.setParameter(parameterIndex, new FloatParameter(x,parameterIndex));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.setParameter(parameterIndex, new DoubleParameter(x,parameterIndex));

    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.setParameter(parameterIndex, new BigDecimalParameter(x,parameterIndex));

    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.setParameter(parameterIndex, new BytesParameter(x,parameterIndex));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.setParameter(parameterIndex, new DateParameter(x,parameterIndex));

    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.setParameter(parameterIndex, new TimeParameter(x,parameterIndex));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.setParameter(parameterIndex, new TimestampParameter(x,parameterIndex));

    }

    @Override
    public void clearParameters() throws SQLException {
        for (int i = 0; i < this.parameters.size(); ++i) {
            this.parameters.set(i,null);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.setParameter(parameterIndex, new DateParameter(x,parameterIndex,cal));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.setParameter(parameterIndex, new TimeParameter(x,parameterIndex,cal));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.setParameter(parameterIndex, new TimestampParameter(x,parameterIndex,cal));
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.setParameter(parameterIndex, new UrlParameter(x,parameterIndex));
    }


    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            var command = new PreparedStatementExecuteQuery(
                    sql,
                    parameters
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
    public boolean execute() throws SQLException {
        try {
            var command = new PreparedStatementExecute(
                    sql,
                    parameters
            );
            var result = (ObjectResult)this.engine.execute(command,connection.getTraceId(),getTraceId());

            return result.getResult();
        }
        catch (SQLException e) {
            throw e;
        }
        catch (Exception e2) {
            throw ExceptionsWrapper.toSQLException(e2);
        }
    }
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.setParameter(parameterIndex, new ArrayParameter(x,parameterIndex));
    }
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.setParameter(parameterIndex, new NClobParameter(value,parameterIndex));
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.setParameter(parameterIndex, new BlobParameter(x,parameterIndex));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.setParameter(parameterIndex, new ClobParameter(x,parameterIndex));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.setParameter(parameterIndex, new ClobParameter(new JdbcClob().fromSource(reader,length),parameterIndex));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        this.setParameter(parameterIndex, new BlobParameter(new JdbcBlob().fromSource(inputStream,length),parameterIndex));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.setParameter(parameterIndex, new ClobParameter(new JdbcNClob().fromSource(reader,length),parameterIndex));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.setParameter(parameterIndex, new ClobParameter(new JdbcClob().fromSource(reader),parameterIndex));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.setParameter(parameterIndex, new BlobParameter(new JdbcBlob().fromSource(inputStream),parameterIndex));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.setParameter(parameterIndex, new ClobParameter(new JdbcNClob().fromSource(reader),parameterIndex));
    }


    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.setParameter(parameterIndex, new SQLXMLParameter(new JdbcSQLXML().fromSqlType(xmlObject),parameterIndex));
    }



    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if(this.metadata==null) {
            metadata= (ResultSetMetaData) engine.execute(new PreparedStatementGetMetaData(), this.connection.getTraceId(), getTraceId());
        }
        return metadata;
    }


    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex,value);
    }


    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.setParameter(parameterIndex, new ObjectParameter(x,parameterIndex,targetSqlType, Integer.MAX_VALUE));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.setParameter(parameterIndex, new ObjectParameter(x,parameterIndex,Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.setParameter(parameterIndex, new ObjectParameter(x,parameterIndex,targetSqlType,scaleOrLength));
    }



    private List<List<PreparedStatementParameter>> batches = new ArrayList<>();
    @Override
    public void addBatch() throws SQLException {
        batches.add(parameters);
        parameters = new ArrayList<>();
    }

    @Override
    public void clearBatch() throws SQLException {
        batches=new ArrayList<>();
        parameters=new ArrayList<>();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new PreparedStatementExecuteBatch(batches),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        setParameter(parameterIndex,new RowIdParameter(x,parameterIndex));
    }

    //TODO implement




    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new UnsupportedOperationException();
    }




    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }



    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
