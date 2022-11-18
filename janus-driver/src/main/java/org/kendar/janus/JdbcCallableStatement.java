package org.kendar.janus;

import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
import org.kendar.janus.cmd.preparedstatement.parameters.*;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.enums.ResultSetConcurrency;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.types.JdbcBlob;
import org.kendar.janus.types.JdbcClob;
import org.kendar.janus.types.JdbcNClob;
import org.kendar.janus.types.JdbcSQLXML;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

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

    //TODO Implements


    @Override
    public boolean wasNull() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }



    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
