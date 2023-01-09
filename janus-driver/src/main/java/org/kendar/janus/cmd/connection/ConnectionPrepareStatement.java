package org.kendar.janus.cmd.connection;

import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.cmd.interfaces.JdbcSqlCommand;
import org.kendar.janus.enums.ResultSetConcurrency;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

public class ConnectionPrepareStatement implements JdbcCommand, JdbcSqlCommand {
    public int getAutoGeneratedKeys() {
        return autoGeneratedKeys;
    }

    public void setAutoGeneratedKeys(int autoGeneratedKeys) {
        this.autoGeneratedKeys = autoGeneratedKeys;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public int[] getColumnIndexes() {
        return columnIndexes;
    }

    public void setColumnIndexes(int[] columnIndexes) {
        this.columnIndexes = columnIndexes;
    }

    public void setType(ResultSetType type) {
        this.type = type;
    }

    public void setConcurrency(ResultSetConcurrency concurrency) {
        this.concurrency = concurrency;
    }

    public void setHoldability(ResultSetHoldability holdability) {
        this.holdability = holdability;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    protected int autoGeneratedKeys;
    protected String[] columnNames;
    protected int[] columnIndexes;

    public ResultSetType getType() {
        return type;
    }

    public ResultSetConcurrency getConcurrency() {
        return concurrency;
    }

    public ResultSetHoldability getHoldability() {
        return holdability;
    }

    public String getSql() {
        return sql;
    }

    protected ResultSetType type;
    protected ResultSetConcurrency concurrency;
    protected ResultSetHoldability holdability;
    protected String sql;

    public ConnectionPrepareStatement(){
        type = ResultSetType.FORWARD_ONLY;
        concurrency = ResultSetConcurrency.CONCUR_READ_ONLY;
        holdability = ResultSetHoldability.DEFAULT;
    }

    public ConnectionPrepareStatement(String sql){
        this.sql = sql;
        type = ResultSetType.FORWARD_ONLY;
        concurrency = ResultSetConcurrency.CONCUR_READ_ONLY;
        holdability = ResultSetHoldability.DEFAULT;
    }

    public ConnectionPrepareStatement withType(ResultSetType type){

        this.type = type;
        return this;
    }

    public ConnectionPrepareStatement withSql(String sql){

        this.sql = sql;
        return this;
    }

    public ConnectionPrepareStatement withConcurrency(ResultSetConcurrency concurrency){

        this.concurrency = concurrency;
        return this;
    }

    public ConnectionPrepareStatement withHoldability(ResultSetHoldability holdability){

        this.holdability = holdability;
        return this;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var connection = (Connection) context.getConnection();
        if (type == null && concurrency == null) {
            return connection.createStatement();
        }
        if(!connection.getMetaData().supportsResultSetType(type.getValue())){
            throw new SQLException(type+" not supported");
        }
        if(!connection.getMetaData().supportsResultSetConcurrency(type.getValue(),concurrency.getValue())){
            throw new SQLException(type+":"+concurrency+" not supported");
        }
        if(holdability!=ResultSetHoldability.DEFAULT){
            if(!connection.getMetaData().supportsResultSetHoldability(holdability.getValue())){
                throw new SQLException(holdability+" not supported");
            }
            return connection.prepareStatement (sql,type.getValue(), concurrency.getValue(),holdability.getValue());
        }
        if(autoGeneratedKeys!=0) {
            return connection.prepareStatement(sql, autoGeneratedKeys);
        }else if(columnNames!=null && columnNames.length>0){
            return connection.prepareStatement(sql, columnNames);
        }else if(columnIndexes!=null && columnIndexes.length>0){
            return connection.prepareStatement(sql, columnIndexes);
        }
         return connection.prepareStatement(sql, type.getValue(),concurrency.getValue());
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("sql",sql);
        builder.write("type",type);
        builder.write("concurrency",concurrency);
        builder.write("holdability",holdability);
        builder.write("columnNames",columnNames);
        builder.write("autoGeneratedKeys",autoGeneratedKeys);
        builder.write("withColumnIndexes",columnIndexes);

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {

        sql =builder.read("sql");
        type =builder.read("type");
        concurrency =builder.read("concurrency");
        holdability =builder.read("holdability");
        columnNames =builder.read("columnNames");
        autoGeneratedKeys =builder.read("autoGeneratedKeys");
        columnIndexes =builder.read("withColumnIndexes");
        return this;
    }

    public ConnectionPrepareStatement withAutoGeneratedKeys(int autoGeneratedKeys) {

        this.autoGeneratedKeys = autoGeneratedKeys;
        return this;
    }

    public ConnectionPrepareStatement withColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
        return this;
    }

    public ConnectionPrepareStatement withColumnIndexes(int[] columnIndexes) {
        this.columnIndexes = columnIndexes;
        return this;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+"{" +
                "\n\tautoGeneratedKeys=" + autoGeneratedKeys +
                ", \n\tcolumnNames=" + Arrays.toString(columnNames) +
                ", \n\tcolumnIndexes=" + Arrays.toString(columnIndexes) +
                ", \n\ttype=" + type +
                ", \n\tconcurrency=" + concurrency +
                ", \n\tholdability=" + holdability +
                ", \n\tsql='" + sql + '\'' +
                '}';
    }
    @Override
    public String getPath() {
        return "/Connection/prepareStatement";
    }
}
