package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

public class DateParameter extends BaseTimeParameter<Date>{
    public DateParameter() {
    }

    public DateParameter(Date value, int columnIndex) {
        super(value, columnIndex);
    }

    public DateParameter(Date value, int parameterIndex, Calendar calendar) {
        super(value,parameterIndex,calendar);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        if(this.calendar==null) {
            preparedStatement.setDate(columnIndex, value);
        }else{
            preparedStatement.setDate(columnIndex, value,calendar);
        }
    }
}
