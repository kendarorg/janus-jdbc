package org.kendar.janus.cmd.preparedstatement.parameters;

import org.kendar.janus.serialization.TypedSerializer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BigDecimalParameter extends SimpleParameter<BigDecimal>{
    private Integer scale;

    public BigDecimalParameter() {
    }


    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        value = value.setScale(0, RoundingMode.HALF_UP);
        preparedStatement.setBigDecimal(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        value = value.setScale(0, RoundingMode.HALF_UP);
        if(hasColumnName())callableStatement.setBigDecimal(columnName,value);
        else load((PreparedStatement) callableStatement);
    }

    public SimpleParameter<BigDecimal> withValue(BigDecimal value){
        this.value = value;
        if(value!=null) {
            this.scale = value.scale();
        }
        return this;
    }


    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("columnIndex",columnIndex);
        builder.write("columnName",columnName);
        builder.write("value",value);
        builder.write("scale",scale);
    }

    @Override
    public Object deserialize(TypedSerializer input) {
        this.columnIndex = input.read("columnIndex");
        this.columnName = input.read("columnName");
        this.value = input.read("value");
        this.scale = input.read("scale");
        return this;
    }
}
