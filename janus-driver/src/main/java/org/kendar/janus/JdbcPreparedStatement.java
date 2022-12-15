package org.kendar.janus;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CharSequenceReader;
import org.kendar.janus.cmd.Exec;
import org.kendar.janus.cmd.preparedstatement.*;
import org.kendar.janus.cmd.preparedstatement.parameters.*;
import org.kendar.janus.cmd.statement.StatementExecuteLargeBatch;
import org.kendar.janus.cmd.statement.StatementExecuteLargeUpdate;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {
    protected String sql;
    protected ResultSetMetaData metadata;

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
        this.setParameter( new NullParameter().withColumnIndex(parameterIndex).withSqlType(sqlType));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.setParameter( new NullParameter().withColumnIndex(parameterIndex)
                .withSqlType(sqlType)
                .withTypeName(typeName));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.setParameter( new StringParameter().withValue(x).withColumnIndex(parameterIndex));
    }

    protected void setParameter(PreparedStatementParameter parameter) {
        parameters.add(parameter);
    }

    @Override
    public int executeUpdate() throws SQLException {
        resultSet=null;
        var result = (ObjectResult)this.engine.execute(
                new PreparedStatementExecuteUpdate(this.sql, this.parameters),
                this.connection.getTraceId(),
                getTraceId());
        return result.getResult();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.setParameter( new BooleanParameter().withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.setParameter( new ByteParameter().withValue(x).withColumnIndex(parameterIndex));

    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.setParameter( new ShortParameter().withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.setParameter( new IntParameter().withValue(x).withColumnIndex(parameterIndex));

    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.setParameter( new LongParameter().withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.setParameter( new FloatParameter().withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.setParameter( new DoubleParameter().withValue(x).withColumnIndex(parameterIndex));

    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.setParameter( new BigDecimalParameter().withValue(x).withColumnIndex(parameterIndex));

    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.setParameter( new BytesParameter().withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.setParameter( new DateParameter().withValue(x).withColumnIndex(parameterIndex));

    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.setParameter( new TimeParameter().withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.setParameter( new TimestampParameter().withValue(x).withColumnIndex(parameterIndex));

    }

    @Override
    public void clearParameters() throws SQLException {
        for (int i = 0; i < this.parameters.size(); ++i) {
            this.parameters.set(i,null);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.setParameter( new DateParameter().withCalendar(cal).withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.setParameter( new TimeParameter().withCalendar(cal).withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.setParameter( new TimestampParameter().withCalendar(cal).withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.setParameter( new UrlParameter().withValue(x).withColumnIndex(parameterIndex));
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
        resultSet=null;
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
        this.setParameter( new ArrayParameter().withValue(x).withColumnIndex(parameterIndex));
    }
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.setParameter( new NClobParameter().withValue(value).withColumnIndex(parameterIndex));
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.setParameter( new BlobParameter().withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.setParameter( new ClobParameter().withValue(x).withColumnIndex(parameterIndex));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.setParameter( new ClobParameter().withValue(new JdbcClob().fromSource(reader,length)).withColumnIndex(parameterIndex));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        this.setParameter( new BlobParameter().withValue(new JdbcBlob().fromSource(inputStream,length)).withColumnIndex(parameterIndex));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.setParameter( new ClobParameter().withValue(new JdbcNClob().fromSource(reader,length)).withColumnIndex(parameterIndex));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.setParameter( new ClobParameter().withValue(new JdbcClob().fromSource(reader)).withColumnIndex(parameterIndex));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.setParameter( new BlobParameter().withValue(new JdbcBlob().fromSource(inputStream)).withColumnIndex(parameterIndex));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.setParameter( new ClobParameter().withValue(new JdbcNClob().fromSource(reader)).withColumnIndex(parameterIndex));
    }


    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.setParameter( new SQLXMLParameter().withValue(new JdbcSQLXML().fromSqlType(xmlObject)).withColumnIndex(parameterIndex));
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
        this.setParameter( new ObjectParameter().withValue(x)
                .withColumnIndex(parameterIndex)
                .withTargetSqlType(targetSqlType));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.setParameter( new ObjectParameter().withValue(x)
                .withColumnIndex(parameterIndex));
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.setParameter( new ObjectParameter().withValue(x)
                .withColumnIndex(parameterIndex)
                .withTargetSqlType(targetSqlType)
                .withScaleOrLength(scaleOrLength));
    }



    private List<List<PreparedStatementParameter>> batches = new ArrayList<>();
    @Override
    public void addBatch() throws SQLException {

        if(parameters==null) {
            parameters= new ArrayList<>();
        }
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
        this.setParameter(new RowIdParameter().withValue(x).withColumnIndex(parameterIndex));
    }


    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        this.setParameter( new CharacterStreamParameter().fromReader(reader,length).withColumnIndex(parameterIndex));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        this.setParameter( new CharacterStreamParameter().fromReader(reader).withColumnIndex(parameterIndex));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.setParameter( new CharacterStreamParameter().fromReader(reader,(int)length).withColumnIndex(parameterIndex));
    }


    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setCharacterStream(parameterIndex,value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex,value,length);
    }

    //TODO implement





    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            setClob(parameterIndex,targetReader,length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBlob(parameterIndex,x,length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            setClob(parameterIndex,targetReader);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setBlob(parameterIndex,x);
    }





    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            setClob(parameterIndex,targetReader,length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.UTF_8));
            setClob(parameterIndex,targetReader,length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBlob(parameterIndex,x,length);
    }




    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        this.setParameter( new RefParameter().withValue(x).withColumnIndex(parameterIndex));
    }


    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new UnsupportedOperationException("?getParameterMetaData");
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "executeLargeUpdate")
                ,connection.getTraceId(),getTraceId())).getResult();
    }


    @Override
    public long executeLargeUpdate(String sql, int columnIndexes[]) throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new PreparedStatementExecuteLargeUpdate(batches,sql,columnIndexes),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }
    @Override
    public long executeLargeUpdate(String sql, String columnNames[])
            throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new PreparedStatementExecuteLargeUpdate(batches,sql,columnNames),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }
    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new PreparedStatementExecuteLargeUpdate(batches,sql,autoGeneratedKeys),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new PreparedStatementExecuteLargeUpdate(batches,sql),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }


    @Override
    public long[] executeLargeBatch() throws SQLException {
        var result= ((ObjectResult)engine.execute(
                new PerparedStatementExecuteLargeBatch(batches),
                connection.getTraceId(),getTraceId()
        ));
        clearBatch();
        return result.getResult();
    }
}
