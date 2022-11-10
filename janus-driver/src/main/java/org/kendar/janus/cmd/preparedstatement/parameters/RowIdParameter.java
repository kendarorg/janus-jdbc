package org.kendar.janus.cmd.preparedstatement.parameters;

import org.apache.commons.lang3.ClassUtils;
import org.kendar.janus.types.JdbcRowId;

import java.sql.PreparedStatement;
import java.sql.RowId;
import java.sql.SQLException;

public class RowIdParameter extends SimpleParameter<RowId>{
    public RowIdParameter() {
    }

    public RowIdParameter(RowId value, int columnIndex) {
        super(value, columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        var rowId = (JdbcRowId)value;
        var originalValue = rowId.getOriginalValue();
        var clazz= originalValue.getClass();
        if(originalValue!=null){
            if(ClassUtils.isAssignable(clazz,long.class)){
                preparedStatement.setLong(columnIndex,(long)originalValue);
            }else if(ClassUtils.isAssignable(clazz,byte[].class)){
                preparedStatement.setBytes(columnIndex,(byte[])originalValue);
            }else if(ClassUtils.isAssignable(clazz,String.class)){
                preparedStatement.setString(columnIndex,(String)originalValue);
            }else{
                throw new SQLException("Type not supported "+clazz.getSimpleName());
            }
        }else {
            preparedStatement.setRowId(columnIndex, value);
        }
    }
}
