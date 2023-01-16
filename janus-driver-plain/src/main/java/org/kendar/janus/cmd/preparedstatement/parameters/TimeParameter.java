package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.*;

public class TimeParameter extends BaseTimeParameter<Time>{
    public TimeParameter() {
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        if(this.calendar==null) {
            preparedStatement.setTime(columnIndex, value);
        }else{
            preparedStatement.setTime(columnIndex, value,calendar);
        }
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName()){
            if(this.calendar==null) {
                callableStatement.setTime(columnName, value);
            }else{
                callableStatement.setTime(columnName, value,calendar);
            }
        }else{
            load((PreparedStatement) callableStatement);
        }
    }
}
