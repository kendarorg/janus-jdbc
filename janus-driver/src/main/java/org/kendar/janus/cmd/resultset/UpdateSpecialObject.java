package org.kendar.janus.cmd.resultset;

import org.apache.commons.lang3.ClassUtils;
import org.kendar.janus.cmd.JdbcCommand;
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
        var connection = (Connection)context.get(connectionId);
        if(ClassUtils.isAssignable(type, NClob.class)){
            updateNClob(resultSet, connection);
        }
        if(ClassUtils.isAssignable(type, Clob.class)){
            updateClob(resultSet, connection);
        }
        if(ClassUtils.isAssignable(type, Blob.class)){
            updateBlob(resultSet,connection);
        }
        if(ClassUtils.isAssignable(type, SQLXML.class)){
            updateSQLXML(resultSet,connection);
        }
        if(ClassUtils.isAssignable(type, Array.class)){
            updateArray(resultSet,connection);
        }
        return null;
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

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        return null;
    }

    @Override
    public String toString() {
        return "UpdateSpecialObject{" +
                "columnName='" + columnName + '\'' +
                ", columnIndex=" + columnIndex +
                ", value=" + value +
                ", type=" + type +
                '}';
    }
}
