package org.kendar.janus;

//import com.google.common.base.Converter;
import org.kendar.util.convert.TypeConverter;
import org.apache.commons.io.IOUtils;
import org.kendar.janus.cmd.Close;
import org.kendar.janus.cmd.Exec;
import org.kendar.janus.cmd.RetrieveRemainingResultSet;
import org.kendar.janus.cmd.resultset.ResultSetGetMetaData;
import org.kendar.janus.cmd.resultset.UpdateSpecialObject;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.enums.ResultSetConcurrency;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.results.ColumnDescriptor;
import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.results.RemainingResultSetResult;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.types.*;
import org.kendar.janus.utils.JdbcTypesConverter;

import java.io.*;
import java.math.BigDecimal;
        import java.math.RoundingMode;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
        import java.util.*;

public class JdbcResultSet implements JdbcResult, ResultSet,TraceAwareType {
    private JdbcResultsetMetaData metadata;
    private Map<String,Integer> labelsToId;
    private Map<String,Integer> namesToId;
    private List<ColumnDescriptor> columnDescriptors;
    private boolean lastRow;
    private int cursor=-1;
    private Engine engine;
    private JdbcConnection connection;
    private int columnCount;
    private boolean closed;
    private int lastColumn = 0;
    private Statement statement;

    public JdbcResultSet(){

    }
    private long traceId;
    private ResultSetType type;
    private ResultSetHoldability holdability;
    private int maxRows;
    private boolean prefetchMetadata;
    private String charset;
    private ResultSetConcurrency concurrency;

    private List<List<Object>> rows;


    public JdbcResultSet(Long traceId, ResultSetType type, int maxRows,
                         boolean prefetchMetadata, String charset, ResultSetConcurrency concurrency,
                         ResultSetHoldability holdability) {

        this.traceId = traceId;
        this.type = type;
        this.holdability = holdability;
        if(type == ResultSetType.SCROLL_SENSITIVE || concurrency == ResultSetConcurrency.CONCUR_UPDATABLE){
            maxRows=1;
        }
        this.maxRows = maxRows;
        this.prefetchMetadata = prefetchMetadata;
        this.charset = charset;
        this.concurrency = concurrency;
    }

    protected void setResultSetMetadata(JdbcResultsetMetaData metadata){
        this.metadata = metadata;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("traceId",traceId);
        builder.write("type",type);
        builder.write("concurrency",concurrency);
        builder.write("holdability",holdability);
        builder.write("maxRows",maxRows);
        builder.write("prefetchMetadata",prefetchMetadata);
        builder.write("charset",charset);
        builder.write("columnDescriptors",columnDescriptors);
        builder.write("lastRow",lastRow);
        builder.write("rows",rows);
        builder.write("metadata",metadata);
    }

    @Override
    public JdbcResult deserialize(TypedSerializer input) {
       traceId= input.read("traceId");
        type= input.read("type");
        concurrency = input.read("concurrency");
        holdability =input.read("holdability");
        maxRows=     input.read("maxRows");
        prefetchMetadata=         input.read("prefetchMetadata");
        charset=                  input.read("charset");
        columnDescriptors = input.read("columnDescriptors");
        lastRow  = input.read("lastRow");
        rows  = input.read("rows");
        metadata =input.read("metadata");
        columnCount = columnDescriptors.size();
        labelsToId = new HashMap<>();
        namesToId = new HashMap<>();
        for (int i = 1; i <= columnCount; ++i) {
            var field =columnDescriptors.get(i-1);
            if(field.getLabel()!=null) {
                labelsToId.put(field.getLabel().toLowerCase(Locale.ROOT), i - 1);
            }
            if(field.getName()!=null) {
                namesToId.put(field.getName().toLowerCase(Locale.ROOT), i - 1);
            }
        }
        return this;
    }


    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public void setConnection(JdbcConnection connection) {
        this.connection = connection;
    }

    public long getTraceId() {
        return traceId;
    }

    public void setTraceId(long traceId) {
        this.traceId = traceId;
    }

    @Override
    public int getType() throws SQLException {
        return type.getValue();
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.maxRows;
    }

    public void intialize(ResultSet rs) throws SQLException {
        var md = rs.getMetaData();
        if (this.prefetchMetadata) {
            this.metadata = new JdbcResultsetMetaData();
            this.metadata.initialize(md);
        }
        columnCount = md.getColumnCount();
        this.columnDescriptors = new ArrayList<>();
        this.labelsToId = new HashMap<>();
        this.namesToId = new HashMap<>();
        for (int i = 1; i <= columnCount; ++i) {

            columnDescriptors.add(new ColumnDescriptor(
                    getColumnType(md,i),
                    md.getColumnName(i),
                    md.getColumnLabel(i)));
            var cd = columnDescriptors.get(i-1);
            if(cd.getLabel()!=null) {
                labelsToId.put(cd.getLabel().toLowerCase(Locale.ROOT), i - 1);
            }
            if(cd.getName()!=null) {
                namesToId.put(cd.getName().toLowerCase(Locale.ROOT), i - 1);
            }
        }

        lastRow=true;
        var rowsCount = 0;
        rows = new ArrayList<>();
        while(rs.next()){

            var row = new ArrayList<>();
            for(var i=0;i<columnDescriptors.size();i++){
                var ob = rs.getObject(i+1);
                row.add(JdbcTypesConverter.convertToSerializable(ob));
            }
            rows.add(row);
            rowsCount++;
            if(rowsCount>=maxRows){
                lastRow=false;
                break;
            }

        }
        if(rowsCount==0){
            lastRow = true;
        }
    }

    private int getColumnType(ResultSetMetaData md,int i) throws SQLException {
        try {
            return md.getColumnType(i);
        }catch (Exception ex){
            return Types.OTHER;
        }
    }

    Object lastColumnValue;
    private Object getFieldById(int columnIndex){
        lastColumnValue = rows.get(cursor).get(columnIndex-1);
        return lastColumnValue;
    }

    private int getFieldId(String columnLabel) throws SQLException {
        columnLabel = columnLabel.toLowerCase(Locale.ROOT);
        var index = labelsToId.get(columnLabel);
        if(index==null){
            index = namesToId.get(columnLabel);
        }
        if(index == null){
            throw new SQLException("Missing field "+columnLabel);
        }
        return index+1;
    }



    private ColumnDescriptor getFieldDataById(int columnIndex){
        lastColumn =columnIndex-1;
        return this.columnDescriptors.get(columnIndex-1);
    }

    private ColumnDescriptor getFieldDataByLabel(String columnLabel){
        columnLabel = columnLabel.toLowerCase(Locale.ROOT);
        var index = labelsToId.get(columnLabel);
        return getFieldDataById(index+1);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null) {
            lastColumnValue= null;
            return null;
        }
        return (URL) result;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getArray(id);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null) {
            lastColumnValue = null;
            return null;
        }
        return (Array)result;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) {
            lastColumnValue = null;
            return false;
        }
        return TypeConverter.convert(Boolean.class,value);

    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) {
            lastColumnValue = null;
            return (byte)0;
        }
        return TypeConverter.convert(byte.class,value);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) {
            lastColumnValue =null;
            return (short)0;
        }
        return TypeConverter.convert(Short.class,value);

    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) {
            lastColumnValue = null;
            return 0;
        }
        return TypeConverter.convert(Integer.class,value);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) {
            lastColumnValue = null;
            return 0L;
        }
        return TypeConverter.convert(Long.class,value);

    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) {
            lastColumnValue = null;
            return 0.0F;
        }
        return TypeConverter.convert(Float.class,value);

    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) {
            lastColumnValue=null;
            return 0.0;
        }
        return TypeConverter.convert(Double.class,value);

    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) {
            lastColumnValue =null;
            return null;
        }
        if(value instanceof BigDecimal){
            return ((BigDecimal)value).setScale(0, RoundingMode.HALF_UP);
        }
        return getBigDecimal(columnIndex,0);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) return null;
        return TypeConverter.convert(BigDecimal.class,value);

    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null){
            lastColumnValue=null;
            return null;
        }
        if(result.getClass()==JdbcBlob.class){
            return ((JdbcBlob)result).getData();
        }
        return (byte[])result;
    }



    @Override
    public Date getDate(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) return null;

        return TypeConverter.convert(Date.class,value);

    }


    @Override
    public Time getTime(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) return null;
        return TypeConverter.convert(Time.class,value);

    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) return null;
        return TypeConverter.convert(Timestamp.class,value);

    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getString(id);
    }


    @Override
    public String getString(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null) return null;
        if(result.getClass()==JdbcClob.class){
            return new String(((JdbcClob)result).getData());
        }else if(result.getClass()==JdbcNClob.class){
            return new String(((JdbcNClob)result).getData());
        }
        return result.toString();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null) return null;
        if(result.getClass()==byte[].class){
            var res = new JdbcBlob();
            res.setBytes(1,(byte[])result);
            return res;
        }
        return (Blob) result;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null) return null;
        if(result.getClass()==String.class){
            var res = new JdbcClob();
            res.setString(1,(String)result);
            return res;
        }
        return (Clob) result;
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null) return null;
        if(result.getClass()==String.class){
            var res = new JdbcNClob();
            res.setString(1,(String)result);
            return res;
        }
        return (NClob)result;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null) return null;
        if(result.getClass()==String.class){
            var res = new JdbcSQLXML();
            res.setString((String)result);
            return res;
        }
        return (SQLXML)result;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null) return null;
        return new StringReader(result.toString());
    }


    @Override
    public String getNString(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null) return null;
        return result.toString();
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getBoolean(id);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getByte(id);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getShort(id);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getInt(id);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getLong(id);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getFloat(id);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getDouble(id);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        var id = getFieldId(columnLabel);
        return getBigDecimal(id,scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getBytes(id);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getDate(id);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getTime(id);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getTimestamp(id);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getFieldById(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getObject(id);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return labelsToId.get(columnLabel)+1;
    }


    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getBigDecimal(id);
    }


    @Override
    public Statement getStatement() throws SQLException {
        return this.statement;
    }

    public void setStatement(Statement statement) {

        this.statement = statement;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T)this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(JdbcResultSet.class);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return lastColumnValue==null;
    }



    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) return null;


        if (!(value instanceof byte[])) {
            if (value instanceof String) {
                try {
                    value = ((String)value).getBytes(this.charset);
                }
                catch (UnsupportedEncodingException e) {
                    throw new SQLException(e);
                }
            }else if(value instanceof NClob){
                try {
                    value = IOUtils.toByteArray(((NClob)value).getCharacterStream(),this.charset);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }else if(value instanceof Clob){
                try {
                    value = IOUtils.toByteArray(((Clob)value).getCharacterStream(),this.charset);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }else if(value instanceof Blob){
                try {
                    value = IOUtils.toByteArray(((Blob)value).getBinaryStream());
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }else {
                throw new SQLException("Can't convert type to inputstream: " + value.getClass());
            }
        }

        String str;

        byte[] cha = (byte[])value;
        try {
            str = new String(cha, 0, cha.length,
                    "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw new SQLException(e);
        }
        return new ByteArrayInputStream(str.getBytes());
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) return null;


        if (!(value instanceof byte[])) {
            if (value instanceof String) {
                try {
                    value = ((String)value).getBytes(this.charset);
                }
                catch (UnsupportedEncodingException e) {
                    throw new SQLException(e);
                }
            }else if(value instanceof NClob){
                try {
                    value = IOUtils.toByteArray(((NClob)value).getCharacterStream(),this.charset);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }else if(value instanceof Clob){
                try {
                    value = IOUtils.toByteArray(((Clob)value).getCharacterStream(),this.charset);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }else if(value instanceof Blob){
                try {
                    value = IOUtils.toByteArray(((Blob)value).getBinaryStream());
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }else {
                throw new SQLException("Can't convert type to inputstream: " + value.getClass());
            }
        }

        return new ByteArrayInputStream((byte[])value);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getBinaryStream(id);
    }



    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getAsciiStream(id);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getUnicodeStream(id);
    }


    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getCharacterStream(id);
    }


    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) return null;
        cal.setTime(this.getDate(columnIndex));
        return new Date(cal.getTimeInMillis());
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        var id = getFieldId(columnLabel);
        return getDate(id,cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) return null;
        cal.setTime(this.getTime(columnIndex));
        return (Time)cal.getTime();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        var id = getFieldId(columnLabel);
        return getTime(id,cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) return null;
        cal.setTime(this.getTime(columnIndex));
        return Timestamp.from(cal.getTime().toInstant());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        var id = getFieldId(columnLabel);
        return getTimestamp(id,cal);
    }



    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        var id = getFieldId(columnLabel);
        return getObject(id,type);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getNCharacterStream(id);
    }


    @Override
    public String getNString(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getNString(id);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getSQLXML(id);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getNClob(id);
    }
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        var id = getFieldId(columnLabel);
        return getObject(id,map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getRef(id);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getBlob(id);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getClob(id);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getURL(id);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        return getRowId(id);
    }

    @Override
    public void close() throws SQLException {
        if (!this.isClosed()) {
            this.engine.execute(new Close(this),this.connection.getTraceId(),this.getTraceId());
            this.closed = true;
        }
        if (this.statement != null && this.statement.isCloseOnCompletion()) {
            this.statement.close();
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return this.cursor < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return this.lastRow && this.cursor == this.rows.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return this.cursor == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return this.lastRow && this.cursor == this.rows.size() - 1;
    }



    @Override
    public boolean next() throws SQLException {
        if(closed)return false;
        if(cursor < (rows.size()-1)){
            cursor++;
            return true;
        }
        if(!lastRow){
            Boolean nextResult = retrieveRemaningResultSet(false);
            if (nextResult == null) return false;
            return nextResult;
        }

        return false;
    }

    private Boolean retrieveRemaningResultSet(boolean isPrevious ) throws SQLException {
        var nextResult = false;

        var result = (RemainingResultSetResult)engine.execute(
                new RetrieveRemainingResultSet(
                        this.columnCount,
                        isPrevious?1:this.maxRows,
                        isPrevious),
                this.connection.getTraceId(),
                this.traceId);
        if(isPrevious){
            rows.set(cursor,result.getRows().get(0));
        }else {
            rows.addAll(result.getRows());
        }
        lastRow = result.isLastRow();

        if(rows == null || rows.size()==0) return null;
        if(cursor < (rows.size()-1)){
            cursor++;
            nextResult = true;
        }
        if(!lastRow){
            nextResult = true;
        }
        return nextResult;
    }



    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        engine.execute(new Exec(this,
                "updateBoolean")
                        .withTypes(int.class,boolean.class)
                        .withParameters(columnIndex,x)
        ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        engine.execute(new Exec(this,
                "updateByte")
                        .withTypes(int.class,byte.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        engine.execute(new Exec(this,
                "updateShort")
                        .withTypes(int.class,short.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        engine.execute(new Exec(this,
                "updateInt")
                        .withTypes(int.class,int.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());

    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        engine.execute(new Exec(this,
                "updateLong")
                        .withTypes(int.class,long.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        engine.execute(new Exec(this,
                "updateFloat")
                        .withTypes(int.class,float.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());

    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        engine.execute(new Exec(this,
                "updateDouble")
                        .withTypes(int.class,double.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        engine.execute(new Exec(this,
                "updateBigDecimal")
                        .withTypes(int.class,BigDecimal.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());

    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        engine.execute(new Exec(this,
                "updateBytes")
                        .withTypes(int.class,byte[].class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());

    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        engine.execute(new Exec(this,
                "updateDate")
                        .withTypes(int.class,Date.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        engine.execute(new Exec(this,
                "updateTime")
                        .withTypes(int.class,Time.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());

    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        engine.execute(new Exec(this,
                "updateTimestamp")
                        .withTypes(int.class,Timestamp.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateString(id,x);
    }



    @Override
    public void updateNull(String columnLabel) throws SQLException {
        var id = getFieldId(columnLabel);
        updateNull(id);

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateBoolean(id,x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateByte(id,x);

    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateShort(id,x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateInt(id,x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateLong(id,x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateFloat(id,x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateDouble(id,x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateBigDecimal(id,x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateBytes(id,x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateDate(id,x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateTime(id,x);

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateTimestamp(id,x);

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        var id = getFieldId(columnLabel);
        updateAsciiStream(id,x,length);

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        var id = getFieldId(columnLabel);
        updateBinaryStream(id,x,length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        var id = getFieldId(columnLabel);
        updateCharacterStream(id,reader,length);

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        var id = getFieldId(columnLabel);
        updateObject(id,x,scaleOrLength);

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        var id = getFieldId(columnLabel);
        updateObject(id,x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        engine.execute(new Exec(this,
                "updateString")
                        .withTypes(int.class,String.class)
                        .withParameters(columnIndex,x)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void beforeFirst() throws SQLException {
        engine.execute(new Exec(this,"beforeFirst"),connection.getTraceId(),getTraceId());
        this.rows = new ArrayList<>();
        this.cursor = -1;
        lastRow = false;
    }

    @Override
    public void updateRow() throws SQLException {
        engine.execute(new Exec(this,"updateRow"),connection.getTraceId(),getTraceId());
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        engine.execute(new Exec(this,"moveToInsertRow"),connection.getTraceId(),getTraceId());
    }


    @Override
    public void insertRow() throws SQLException {
        engine.execute(new Exec(this,"insertRow"),connection.getTraceId(),getTraceId());
    }

    @Override
    public void deleteRow() throws SQLException {
        engine.execute(new Exec(this,"deleteRow"),connection.getTraceId(),getTraceId());
    }

    private void mustBeUpdatable() throws SQLException {
        if(concurrency!=ResultSetConcurrency.CONCUR_UPDATABLE){
            throw new SQLException("Wrong concurrency");
        }
    }

    @Override
    public void refreshRow() throws SQLException {
        mustBeUpdatable();
        engine.execute(new Exec(this,"refreshRow"),connection.getTraceId(),getTraceId());

    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        mustBeUpdatable();
        engine.execute(new Exec(this,"cancelRowUpdates"),connection.getTraceId(),getTraceId());

    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        engine.execute(new Exec(this,"moveToCurrentRow"),connection.getTraceId(),getTraceId());

    }


    @Override
    public boolean rowUpdated() throws SQLException {
        mustBeUpdatable();
        var obResult = (ObjectResult)engine.execute(new Exec(this,"rowUpdated")
                ,connection.getTraceId(),getTraceId());
        return obResult.getResult();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        mustBeUpdatable();
        var obResult = (ObjectResult)engine.execute(new Exec(this,"rowInserted"),connection.getTraceId(),getTraceId());
        return obResult.getResult();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        mustBeUpdatable();
        var obResult = (ObjectResult)engine.execute(new Exec(this,"rowDeleted"),connection.getTraceId(),getTraceId());
        return obResult.getResult();
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public void afterLast() throws SQLException {
        while (this.next()) {}
        this.cursor = this.rows.size();
    }

    @Override
    public boolean first() throws SQLException {
        if(this.cursor==-1){ return this.next();}
        if(this.cursor==0){ return true;}
        mustNotBeForwardOnly();
        this.beforeFirst();
        return this.next();

    }

    private void mustNotBeForwardOnly() throws SQLException {
        if(this.type == ResultSetType.FORWARD_ONLY){
            throw new SQLException("Forward only");
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public boolean last() throws SQLException {
        while (this.next()) {}
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        var res = this.cursor + 1;
        return res;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return concurrency.getValue();
    }


    @Override
    public int getHoldability() throws SQLException {
        return holdability.getValue();
    }



    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (this.metadata == null) {
            this.metadata = (JdbcResultsetMetaData) this.engine.execute(new ResultSetGetMetaData(),connection.getTraceId(),getTraceId());
        }
        return this.metadata;
    }



    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        var value = getFieldById(columnIndex);
        if(value==null) return null;


        if (!(value instanceof byte[])) {
            if (value instanceof String) {
                try {
                    value = ((String)value).getBytes(this.charset);
                }
                catch (UnsupportedEncodingException e) {
                    throw new SQLException(e);
                }
            }else if(value instanceof NClob){
                try {
                    value = IOUtils.toByteArray(((NClob)value).getCharacterStream(),this.charset);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }else if(value instanceof Clob){
                try {
                    value = IOUtils.toByteArray(((Clob)value).getCharacterStream(),this.charset);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }else if(value instanceof Blob){
                try {
                    value = IOUtils.toByteArray(((Blob)value).getBinaryStream());
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }else {
                throw new SQLException("Can't convert type to inputstream: " + value.getClass());
            }
        }

        String str;

        byte[] cha = (byte[])value;
        try {
            str = new String(cha, 0, cha.length,
                    "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SQLException(e);
        }
        return new ByteArrayInputStream(str.getBytes());
    }



    @Override
    public <T> T getObject(int columnIndex, Class<T> var0) throws SQLException {
        var var1 = getFieldById(columnIndex);
        if(var1==null) return null;
        return TypeConverter.convert(var0,var1);



    }


    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        updateString(columnIndex,nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        updateString(columnLabel,nString);
    }
    



    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public boolean absolute(int row) throws SQLException {
        row-=1;

        if(row<cursor) {
            mustNotBeForwardOnly();
        }
        while(cursor<row){
            if(!next()){
                return false;
            }
        }
        return true;
    }


    @Override
    public boolean relative(int rows) throws SQLException {
        if(rows ==0){
            return true;
        }
        if(rows>0){
            var maxPos = cursor+rows;
            while(cursor<maxPos){
                if(!next()){
                    return false;
                }
            }
            return true;
        }else if(rows<0) {
            mustNotBeForwardOnly();
            while(rows<0) {
                var res = this.previous();
                if(cursor==0){
                    return false;
                }
                if(res==false) {
                    return false;
                }
                rows++;
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        mustNotBeForwardOnly();
        boolean result = ((ObjectResult)engine.execute(new Exec(this,
                        "previous")
                ,connection.getTraceId(),getTraceId())).getResult();
        if(result==false)return false;
        var recursor = cursor;
        recursor--;;
        retrieveRemaningResultSet(true);
        cursor = recursor;
        return true;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        engine.execute(new Exec(this,
                        "setFetchDirection")
                        .withTypes(int.class)
                        .withParameters(direction)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getFetchDirection")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

        engine.execute(new Exec(this,
                        "setFetchSize")
                        .withTypes(int.class)
                        .withParameters(rows)
                ,connection.getTraceId(),getTraceId());
    }




    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        var result = getFieldById(columnIndex);
        if(result==null) return null;
        if(result instanceof RowId) return (RowId) result;
        return new JdbcRowId().fromObject(result);
    }


    @Override
    public void updateNull(int columnIndex) throws SQLException {
        engine.execute(new Exec(this,
                        "updateNull")
                        .withTypes(int.class)
                        .withParameters(columnIndex)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            updateClob(columnIndex,targetReader,length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        updateBlob(columnIndex,x,length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        updateClob(columnIndex,x,length);
    }




    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withIndex(columnIndex)
                        .withValue(x,Blob.class)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withName(columnLabel)
                        .withValue(x,Blob.class)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withIndex(columnIndex)
                        .withValue(x,Clob.class)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withName(columnLabel)
                        .withValue(x,Clob.class)
                ,connection.getTraceId(),getTraceId());
    }
    @Override
    public void updateNClob(int columnIndex, NClob x) throws SQLException {
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withIndex(columnIndex)
                        .withValue(x,Blob.class)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateNClob(String columnLabel, NClob x) throws SQLException {
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withName(columnLabel)
                        .withValue(x,NClob.class)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML x) throws SQLException {
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withIndex(columnIndex)
                        .withValue(x,SQLXML.class)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML x) throws SQLException {
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withName(columnLabel)
                        .withValue(x,SQLXML.class)
                ,connection.getTraceId(),getTraceId());
    }



    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        updateBlob(columnIndex,new JdbcBlob().fromSource(inputStream,length));
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        updateBlob(columnLabel,new JdbcBlob().fromSource(inputStream,length));
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        updateClob(columnIndex,new JdbcClob().fromSource(reader,length));
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(columnLabel,new JdbcClob().fromSource(reader,length));
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        updateClob(columnIndex,new JdbcNClob().fromSource(reader,length));
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(columnLabel,new JdbcNClob().fromSource(reader,length));
    }
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        updateBlob(columnIndex,new JdbcBlob().fromSource(inputStream));
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        updateBlob(columnLabel,new JdbcBlob().fromSource(inputStream));
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        updateClob(columnIndex,new JdbcClob().fromSource(reader));
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(columnLabel,new JdbcClob().fromSource(reader));
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        updateNClob(columnIndex,new JdbcNClob().fromSource(reader));
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateNClob(columnLabel,new JdbcNClob().fromSource(reader));
    }


    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            updateClob(columnIndex,targetReader,length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            updateClob(columnLabel,targetReader,length);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }



    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            updateClob(columnIndex,targetReader);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        try {
            var buffer = IOUtils.toByteArray(x);
            Reader targetReader =  new StringReader(new String(buffer, StandardCharsets.US_ASCII));
            updateClob(columnLabel,targetReader);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        updateCharacterStream(columnIndex,x,length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader x, long length) throws SQLException {
        updateCharacterStream(columnLabel,x,length);
    }
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        updateCharacterStream(columnIndex,x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader x) throws SQLException {
        updateCharacterStream(columnLabel,x);
    }


    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateClob(columnLabel,reader);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        updateClob(columnIndex,x,length);
    }
    @Override
    public void updateCharacterStream(String columnLabel, Reader x, long length) throws SQLException {
        updateClob(columnLabel,x,length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        updateBlob(columnIndex,x,length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateBlob(columnLabel,x,length);
    }


    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        updateBlob(columnIndex,x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        updateClob(columnIndex,x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBlob(columnLabel,x);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        var rs = getClob(columnIndex);
        if(rs==null){
            lastColumnValue =null;
            return null;
        }
        return rs.getCharacterStream();
    }


    @Override
    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException("getCursorName");
    }



    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        if(x==null){
            updateNull(columnIndex);
            return;
        }
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withIndex(columnIndex)
                        .withValue(x,x.getClass())
                        .withScaleOrLength(scaleOrLength)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        if(x==null){
            updateNull(columnIndex);
            return;
        }
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withIndex(columnIndex)
                        .withValue(x,x.getClass())
                ,connection.getTraceId(),getTraceId());
    }


    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("?getObject map");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("?getRef");
    }






    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withIndex(columnIndex)
                        .withValue(x,Array.class)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        engine.execute(new UpdateSpecialObject(connection.getTraceId())
                        .withName(columnLabel)
                        .withValue(x,Array.class)
                ,connection.getTraceId(),getTraceId());
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException("updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException("updateRowId");
    }
}
