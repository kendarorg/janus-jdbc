package org.kendar.janus.cmd.resultset;

import org.apache.commons.lang3.ClassUtils;
import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;
import org.kendar.janus.types.*;

import java.io.IOException;
import java.sql.*;

public class UpdateSpecialObject  implements JdbcCommand {
    private String columnName;
    private int columnIndex;
    private Object value;
    private Class<?> type;
    private long connectionId;
    private Integer scaleOrLength;

    public UpdateSpecialObject withValue(Object value,Class<?> type){
        this.value = value;
        this.type = type;
        return this;
    }

    public UpdateSpecialObject withIndex(int columnIndex){
        this.columnIndex = columnIndex;
        return this;
    }

    public UpdateSpecialObject withName(String columnName){
        this.columnName = columnName;
        return this;
    }

    public UpdateSpecialObject(){

    }

    public UpdateSpecialObject(long connectionId){

        this.connectionId = connectionId;
    }

    protected boolean hasColumnName(){
        return columnName!=null && !columnName.isEmpty();
    }
    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {

        var resultSet = (ResultSet)context.get(uid);
        var connection = (Connection)context.getConnection();;
        if(value ==null){
            updateNull(resultSet,connection);
        }else if(scaleOrLength!=null){
            if(hasColumnName()) resultSet.updateObject(columnName,value,scaleOrLength);
            else resultSet.updateObject(columnIndex,value,scaleOrLength);
        }else if(ClassUtils.isAssignable(type, NClob.class)){
            updateNClob(resultSet, connection);
        }else if(ClassUtils.isAssignable(type, Clob.class)){
            updateClob(resultSet, connection);
        }else if(ClassUtils.isAssignable(type, Blob.class)){
            updateBlob(resultSet,connection);
        }else if(ClassUtils.isAssignable(type, SQLXML.class)){
            updateSQLXML(resultSet,connection);
        }else if(ClassUtils.isAssignable(type, Array.class)){
            updateArray(resultSet,connection);
        }
        return null;
    }

    private void updateNull(ResultSet resultSet, Connection connection) throws SQLException {
        if(hasColumnName()) resultSet.updateNull(columnName);
        else resultSet.updateNull(columnIndex);
    }

    private void updateArray(ResultSet resultSet, Connection connection) throws SQLException{
        var janusObject = (JdbcArray)value;
        var driverObject = connection.createArrayOf(janusObject.getBaseTypeName(), (Object[]) janusObject.getArray());

        if(hasColumnName()) resultSet.updateArray(columnName,driverObject);
        else resultSet.updateArray(columnIndex,driverObject);
    }

    private void updateSQLXML(ResultSet resultSet, Connection connection) throws SQLException{
        var janusObject = (JdbcSQLXML)value;
        var driverObject = connection.createSQLXML();
        try(var cs =driverObject.setCharacterStream()){
            cs.write(janusObject.getData());
        } catch (IOException e) {
            throw new SQLException(e);
        }
        if(hasColumnName()) resultSet.updateSQLXML(columnName,driverObject);
        else resultSet.updateSQLXML(columnIndex,driverObject);
    }

    private void updateBlob(ResultSet resultSet, Connection connection) throws SQLException{
        var janusObject = (JdbcBlob)value;
        var driverObject = connection.createBlob();
        try(var cs =driverObject.setBinaryStream(0)){
            cs.write(janusObject.getData());
        } catch (IOException e) {
            throw new SQLException(e);
        }
        if(hasColumnName()) resultSet.updateBlob(columnName,driverObject);
        else resultSet.updateBlob(columnIndex,driverObject);
    }

    private void updateClob(ResultSet resultSet, Connection connection)  throws SQLException{
        var janusObject = (JdbcClob)value;
        var driverObject = connection.createClob();
        try(var cs =driverObject.setCharacterStream(0)){
            cs.write(janusObject.getData());
        } catch (IOException e) {
            throw new SQLException(e);
        }
        if(hasColumnName()) resultSet.updateClob(columnName,driverObject);
        else resultSet.updateClob(columnIndex,driverObject);
    }

    private void updateNClob(ResultSet resultSet, Connection connection) throws SQLException {
        var janusObject = (JdbcNClob)value;
        var driverObject = connection.createNClob();
        try(var cs =driverObject.setCharacterStream(0)){
            cs.write(janusObject.getData());
        } catch (IOException e) {
            throw new SQLException(e);
        }
        if(hasColumnName()) resultSet.updateNClob(columnName,driverObject);
        else resultSet.updateNClob(columnIndex,driverObject);
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("type",type);
        builder.write("columnName",columnName);
        builder.write("value",value);
        builder.write("scaleOrLength",scaleOrLength);
        builder.write("columnIndex",columnIndex);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        type = builder.read("type");
        columnName = builder.read("columnName");
        value = builder.read("value");
        scaleOrLength = builder.read("scaleOrLength");
        columnIndex = builder.read("columnIndex");
        return null;
    }

    @Override
    public String toString() {
        return "UpdateSpecialObject{" +
                "columnName='" + columnName + '\'' +
                ", columnIndex=" + columnIndex +
                ", value=" + value +
                ", scaleOrLength=" + scaleOrLength +
                ", type=" + type +
                '}';
    }

    public JdbcCommand withScaleOrLength(int scaleOrLength) {
        this.scaleOrLength = scaleOrLength;
        return this;
    }

    @Override
    public String getPath(){
        var result = "/ResultSet/";
        if(value ==null){
            result+="updateNull";
        }else if(scaleOrLength!=null){
            result+="updateObject";
        }else if(ClassUtils.isAssignable(type, NClob.class)){
            result+="updateNClob";
        }else if(ClassUtils.isAssignable(type, Clob.class)){
            result+="updateClob";
        }else if(ClassUtils.isAssignable(type, Blob.class)){
            result+="updateBlob";
        }else if(ClassUtils.isAssignable(type, SQLXML.class)){
            result+="updateSQLXML";
        }else if(ClassUtils.isAssignable(type, Array.class)){
            result+="updateArray";
        }
        return result;

    }
}
