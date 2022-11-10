package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

public class TimestampParameter extends BaseTimeParameter<Timestamp>{


    public TimestampParameter() {
    }

    public TimestampParameter(Timestamp value, int columnIndex) {
        super(value, columnIndex);
    }

    public TimestampParameter(Timestamp value, int parameterIndex, Calendar calendar) {
        super(value,parameterIndex,calendar);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        if(this.calendar==null) {
            preparedStatement.setTimestamp(columnIndex, value);
        }else{
            preparedStatement.setTimestamp(columnIndex, value,calendar);
        }
    }
}
