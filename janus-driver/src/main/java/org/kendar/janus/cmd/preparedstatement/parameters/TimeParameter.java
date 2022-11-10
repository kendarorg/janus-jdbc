package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Calendar;

public class TimeParameter extends BaseTimeParameter<Time>{
    public TimeParameter() {
    }

    public TimeParameter(Time value, int columnIndex) {
        super(value, columnIndex);
    }

    public TimeParameter(Time value, int parameterIndex, Calendar calendar) {
        super(value,parameterIndex,calendar);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        if(this.calendar==null) {
            preparedStatement.setTime(columnIndex, value);
        }else{
            preparedStatement.setTime(columnIndex, value,calendar);
        }
    }
}
