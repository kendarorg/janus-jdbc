package org.kendar.janus;

import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.utils.SqlExceptionSupplier;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcResultsetMetaData implements JdbcResult,ResultSetMetaData {
    private int columnCount;
    private String[] catalogName;
    private String[] schemaName;
    private String[] tableName;
    private String[] columnClassName;
    private String[] columnLabel;
    private String[] columnName;
    private String[] columnTypeName;
    private Integer[] columnType;
    private Integer[] columnDisplaySize;
    private Integer[] precision;
    private Integer[] scale;
    private Integer[] nullable;
    private Boolean[] autoIncrement;
    private Boolean[] caseSensitive;
    private Boolean[] currency;
    private Boolean[] readOnly;
    private Boolean[] searchable;
    private Boolean[] signed;
    private Boolean[] writable;
    private Boolean[] definitivelyWritable;



    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("columnCount",columnCount);
        builder.write("catalogName",catalogName);
        builder.write("schemaName",schemaName);
        builder.write("tableName",tableName);
        builder.write("columnClassName",columnClassName);
        builder.write("columnLabel",columnLabel);
        builder.write("columnName",columnName);
        builder.write("columnTypeName",columnTypeName);
        builder.write("columnDisplaySize",columnDisplaySize);
        builder.write("columnType",columnType);
        builder.write("precision",precision);
        builder.write("scale",scale);
        builder.write("nullable",nullable);
        builder.write("autoIncrement",autoIncrement);
        builder.write("caseSensitive",caseSensitive);
        builder.write("currency",currency);
        builder.write("readOnly",readOnly);
        builder.write("searchable",searchable);
        builder.write("signed",signed);
        builder.write("writable",writable);
        builder.write("definitivelyWritable",definitivelyWritable);


    }

    @Override
    public JdbcResult deserialize(TypedSerializer builder) {
        columnCount =builder.read("columnCount");
        catalogName=builder.read("catalogName");
        schemaName=builder.read("schemaName");
        tableName=builder.read("tableName");
        columnClassName=builder.read("columnClassName");
        columnLabel=builder.read("columnLabel");
        columnName=builder.read("columnName");
        columnTypeName=builder.read("columnTypeName");
        columnDisplaySize=builder.read("columnDisplaySize");
        columnType=builder.read("columnType");
        precision=builder.read("precision");
        scale=builder.read("scale");
        nullable=builder.read("nullable");
        autoIncrement=builder.read("autoIncrement");
        caseSensitive=builder.read("caseSensitive");
        currency=builder.read("currency");
        readOnly=builder.read("readOnly");
        searchable=builder.read("searchable");
        signed=builder.read("signed");
        writable=builder.read("writable");
        definitivelyWritable=builder.read("definitivelyWritable");
        return this;
    }

    public void initialize(ResultSetMetaData rsmd) throws SQLException {
        columnCount = rsmd.getColumnCount();

        catalogName = new String[columnCount];
        schemaName = new String[columnCount];
        tableName = new String[columnCount];
        columnClassName = new String[columnCount];
        columnLabel = new String[columnCount];
        columnName = new String[columnCount];
        columnTypeName = new String[columnCount];
        columnDisplaySize = new Integer[columnCount];
        columnType = new Integer[columnCount];
        precision = new Integer[columnCount];
        scale = new Integer[columnCount];
        nullable = new Integer[columnCount];
        autoIncrement = new Boolean[columnCount];
        caseSensitive = new Boolean[columnCount];
        currency = new Boolean[columnCount];
        readOnly = new Boolean[columnCount];
        searchable = new Boolean[columnCount];
        signed = new Boolean[columnCount];
        writable = new Boolean[columnCount];
        definitivelyWritable = new Boolean[columnCount];

        for (int i = 0; i < this.columnCount; ++i) {
            var col = i+1;
            addToArray(catalogName,i,()->rsmd.getCatalogName(col));
            addToArray(schemaName,i,()->rsmd.getSchemaName(col));
            addToArray(tableName,i,()->rsmd.getTableName(col));
            addToArray(columnClassName,i,()->rsmd.getColumnClassName(col));
            addToArray(columnLabel,i,()->rsmd.getColumnLabel(col));
            addToArray(columnName,i,()->rsmd.getColumnName(col));
            addToArray(columnTypeName,i,()->rsmd.getColumnTypeName(col));
            addToArray(columnDisplaySize,i,()->rsmd.getColumnDisplaySize(col));
            addToArray(columnType,i,()->rsmd.getColumnType(col));
            addToArray(precision,i,()->rsmd.getPrecision(col));
            addToArray(scale,i,()->rsmd.getScale(col));
            addToArray(nullable,i,()->rsmd.isNullable(col));
            addToArray(autoIncrement,i,()->rsmd.isAutoIncrement(col));
            addToArray(caseSensitive,i,()->rsmd.isCaseSensitive(col));
            addToArray(currency,i,()->rsmd.isCurrency(col));
            addToArray(readOnly,i,()->rsmd.isReadOnly(col));
            addToArray(searchable,i,()->rsmd.isSearchable(col));
            addToArray(signed,i,()->rsmd.isSigned(col));
            addToArray(writable,i,()->rsmd.isWritable(col));
            addToArray(definitivelyWritable,i,()->rsmd.isDefinitelyWritable(col));
        }
    }

    private <T> void addToArray(T[] dest, int index, SqlExceptionSupplier<T> supplier) {
        try {
            dest[index] = supplier.get();
        }catch (SQLException ex){
            dest[index] = null;
        }
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnCount;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return autoIncrement[column-1];
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return caseSensitive[column-1];
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return searchable[column-1];
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return currency[column-1];
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return nullable[column-1];
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return signed[column-1];
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return columnDisplaySize[column-1];
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columnLabel[column-1];
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnName[column-1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return schemaName[column-1];
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return precision[column-1];
    }

    @Override
    public int getScale(int column) throws SQLException {
        return scale[column-1];
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return tableName[column-1];
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return catalogName[column-1];
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return columnType[column-1];
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return columnTypeName[column-1];
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return readOnly[column-1];
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return writable[column-1];
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return definitivelyWritable[column-1];
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return columnClassName[column-1];
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T)this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(JdbcResultsetMetaData.class);
    }
}
