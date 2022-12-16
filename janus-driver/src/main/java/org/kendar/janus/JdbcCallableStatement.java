package org.kendar.janus;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CharSequenceReader;
import org.kendar.janus.cmd.call.CallableStatementExecute;
import org.kendar.janus.cmd.call.CallableStatementExecuteQuery;
import org.kendar.janus.cmd.preparedstatement.PreparedStatementExecute;
import org.kendar.janus.cmd.preparedstatement.PreparedStatementExecuteQuery;
import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
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
import org.kendar.janus.utils.SqlExceptionFunction;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public class JdbcCallableStatement extends JdbcPreparedStatement  implements CallableStatement {
    public JdbcCallableStatement(JdbcConnection connection, Engine engine, long traceId, int maxRows, int queryTimeout,
                                 ResultSetType resultSetType, ResultSetConcurrency concurrency, ResultSetHoldability holdability) {
        super(connection, engine, traceId, maxRows, queryTimeout, resultSetType, concurrency,holdability);
        outParameters = new ArrayList<>();
    }


    protected List<PreparedStatementParameter> outParameters;
    private void setOutParameter(PreparedStatementParameter parameter) {
        outParameters.add(parameter);
    }
    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        this.setOutParameter(new ObjectParameter()
                .asOutParameter()
                .withTargetSqlType(sqlType)
                .withColumnIndex(parameterIndex));
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        this.setOutParameter(new ObjectParameter()
                .asOutParameter()
                .withTargetSqlType(sqlType)
                .withColumnIndex(parameterIndex)
                .withScaleOrLength(scale));
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.setOutParameter(new ObjectParameter()
                .asOutParameter()
                .withTargetSqlType(sqlType)
                .withColumnIndex(parameterIndex)
                .withTypeName(typeName));
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        this.setOutParameter(new ObjectParameter()
                .asOutParameter()
                .withTargetSqlType(sqlType)
                .withColumnName(parameterName));
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        this.setOutParameter(new ObjectParameter()
                .asOutParameter()
                .withTargetSqlType(sqlType)
                .withColumnName(parameterName)
                .withScaleOrLength(scale));
    }


    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        this.setParameter( new ObjectParameter().withValue(x)
                .withColumnName(parameterName)
                .withTargetSqlType(targetSqlType)
                .withScaleOrLength(scale));
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        this.setParameter( new ObjectParameter().withValue(x)
                .withColumnName(parameterName)
                .withTargetSqlType(targetSqlType));
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        this.setParameter( new ObjectParameter().withValue(x)
                .withColumnName(parameterName));
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        this.setParameter( new CharacterStreamParameter().fromReader(reader,length).withColumnName(parameterName));
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        this.setParameter( new DateParameter().withCalendar(cal).withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        this.setParameter( new TimeParameter().withCalendar(cal).withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        this.setParameter( new TimestampParameter().withCalendar(cal).withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        this.setParameter( new NullParameter().withColumnName(parameterName)
                .withSqlType(sqlType)
                .withTypeName(typeName));
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        setString(parameterName,value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        setCharacterStream(parameterName,value);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        this.setParameter( new NClobParameter().withValue(value).withColumnName(parameterName));
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        this.setParameter( new ClobParameter().withValue(new JdbcNClob().fromSource(reader,length)).withColumnName(parameterName));
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        this.setParameter( new BlobParameter().withValue(new JdbcBlob().fromSource(inputStream)).withColumnName(parameterName));
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        this.setParameter( new ClobParameter().withValue(new JdbcNClob().fromSource(reader)).withColumnName(parameterName));
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        this.setParameter( new SQLXMLParameter().withValue(new JdbcSQLXML().fromSqlType(xmlObject)).withColumnName(parameterName));
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        this.setParameter( new BlobParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        this.setParameter( new ClobParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        this.setParameter( new CharacterStreamParameter().fromReader(reader).withColumnName(parameterName));
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        setCharacterStream(parameterName,value);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        this.setParameter( new ClobParameter().withValue(new JdbcClob().fromSource(reader)).withColumnName(parameterName));
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        this.setParameter( new BlobParameter().withValue(new JdbcBlob().fromSource(inputStream)).withColumnName(parameterName));
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        this.setParameter( new ClobParameter().withValue(new JdbcNClob().fromSource(reader)).withColumnName(parameterName));
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        this.setOutParameter(new ObjectParameter()
                .asOutParameter()
                .withTargetSqlType(sqlType)
                .withColumnName(parameterName)
                .withTypeName(typeName));
    }



    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        this.setParameter( new UrlParameter().withValue(val).withColumnName(parameterName));
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        this.setParameter( new NullParameter().withColumnName(parameterName).withSqlType(sqlType));
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        this.setParameter( new BooleanParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        this.setParameter( new ByteParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        this.setParameter( new ShortParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        this.setParameter( new IntParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        this.setParameter( new LongParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        this.setParameter( new FloatParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        this.setParameter( new DoubleParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        this.setParameter( new BigDecimalParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        this.setParameter( new StringParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        this.setParameter( new BytesParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        this.setParameter( new DateParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        this.setParameter( new TimeParameter().withValue(x).withColumnName(parameterName));
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        this.setParameter( new TimestampParameter().withValue(x).withColumnName(parameterName));
    }

    private void setupResultSet() throws SQLException {
        if(resultSet==null){
            resultSet = getResultSet();
        }
        if (resultSet.isBeforeFirst()) {
            resultSet.next();
        }
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getString(a));
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getBoolean(a));
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getByte(a));
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getShort(a));
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            var command = new CallableStatementExecuteQuery(
                    sql,
                    parameters,
                    outParameters
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
            var command = new CallableStatementExecute(
                    sql,
                    parameters,
                    outParameters
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

    private <T> T  getParamByIndex(int parameterIndex, SqlExceptionFunction<Integer,T> retrieve ) throws SQLException {
        setupResultSet();
        if(resultSet!=null) {
            var possible = retrieve.apply(parameterIndex);
            return possible;
        }
        var par = getParameter(parameterIndex);
        return (T)par.getValue();
    }

    private <T> T  getParamByName(String parameterName, SqlExceptionFunction<String,T> retrieve ) throws SQLException {
        setupResultSet();
        if(resultSet!=null) {
            var possible = retrieve.apply(parameterName);
            return possible;
        }
        var par = getParameter(parameterName);
        return (T)par.getValue();
    }
    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getInt(a));
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getLong(a));
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getFloat(a));
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getDouble(a));
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getBigDecimal(a,scale));
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getBytes(a));
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getDate(a));
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getTime(a));
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getTimestamp(a));
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getObject(a));
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getBigDecimal(a));
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getObject(a,map));
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getRef(a));
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getBlob(a));
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getClob(a));
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getArray(a));
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getDate(a,cal));
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getTime(a,cal));
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getTimestamp(a,cal));
    }



    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex, a -> resultSet.getURL(a));
    }

    private PreparedStatementParameter getParameter(String parameterName){
        var col = this.parameters.stream().filter(p->parameterName.equalsIgnoreCase(p.getColumnName())).findFirst();
        if(col.isPresent())return col.get();
        return null;
    }


    private PreparedStatementParameter getParameter(int parameterIndex){
        var col = this.parameters.stream().filter(p->parameterIndex==p.getColumnIndex()).findFirst();
        if(col.isPresent())return col.get();
        return null;
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getString(a));
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getBoolean(a));
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getByte(a));
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getShort(a));
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getInt(a));
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getLong(a));
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getFloat(a));
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getDouble(a));
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getBytes(a));
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getDate(a));
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getTime(a));
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getTimestamp(a));
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getObject(a));
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getBigDecimal(a));
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getObject(a,map));
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getRef(a));
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getBlob(a));
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getClob(a));
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getArray(a));
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getDate(a,cal));
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getTime(a,cal));
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getTimestamp(a,cal));
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getURL(a));
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getRowId(a));
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return getParamByName(parameterName,a->resultSet.getRowId(a));
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getNClob(a));
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return getParamByName(parameterName, a -> resultSet.getNClob(a));
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getSQLXML(a));
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return getParamByName(parameterName, a -> resultSet.getSQLXML(a));
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getNString(a));
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return getParamByName(parameterName, a -> resultSet.getNString(a));
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getNCharacterStream(a));
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return getParamByName(parameterName, a -> resultSet.getNCharacterStream(a));
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getCharacterStream(a));
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return getParamByName(parameterName, a -> resultSet.getCharacterStream(a));
    }


    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return getParamByIndex(parameterIndex,a->resultSet.getObject(a,type));
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return getParamByName(parameterName, a -> resultSet.getObject(a,type));
    }


    @Override
    public boolean wasNull() throws SQLException {
        setupResultSet();
        return resultSet.wasNull();
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        this.setParameter(new RowIdParameter().withValue(x).withColumnName(parameterName));
    }
    


    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            setClob(parameterName,targetReader,length);
        } catch (IOException e) {
            throw new SQLException(e);
        }

    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setBlob(parameterName,x,length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            setClob(parameterName,targetReader,length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        setBlob(parameterName,x,length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        setClob(parameterName,reader,length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            setClob(parameterName,targetReader);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        setBlob(parameterName,x);
    }
}
