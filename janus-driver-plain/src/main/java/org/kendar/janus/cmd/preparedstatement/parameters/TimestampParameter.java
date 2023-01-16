package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TimestampParameter extends BaseTimeParameter<Timestamp>{


    public TimestampParameter() {
    }


    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        if(this.calendar==null) {
            preparedStatement.setTimestamp(columnIndex, value);
        }else{
            preparedStatement.setTimestamp(columnIndex, value,calendar);
        }
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName()){
            if(this.calendar==null) {
                callableStatement.setTimestamp(columnName, value);
            }else{
                callableStatement.setTimestamp(columnName, value,calendar);
            }
        }else{
            load((PreparedStatement) callableStatement);
        }
    }
}
