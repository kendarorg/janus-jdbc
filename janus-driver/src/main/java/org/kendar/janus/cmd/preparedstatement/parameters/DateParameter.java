package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

public class DateParameter extends BaseTimeParameter<Date>{
    public DateParameter() {
    }


    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        if(this.calendar==null) {
            preparedStatement.setDate(columnIndex, value);
        }else{
            preparedStatement.setDate(columnIndex, value,calendar);
        }
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName()){
            if(this.calendar==null) {
                callableStatement.setDate(columnName, value);
            }else{
                callableStatement.setDate(columnName, value,calendar);
            }
        }else{
            load((PreparedStatement) callableStatement);
        }
    }
}
