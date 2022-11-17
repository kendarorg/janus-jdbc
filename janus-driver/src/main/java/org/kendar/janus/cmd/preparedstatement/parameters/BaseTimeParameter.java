package org.kendar.janus.cmd.preparedstatement.parameters;

import org.kendar.janus.serialization.TypedSerializer;

import java.util.Calendar;

public abstract class BaseTimeParameter<T> extends SimpleParameter<T>{
    protected Calendar calendar;

    public BaseTimeParameter(){

    }

    public BaseTimeParameter(T value, int parameterIndex) {
        super(value,parameterIndex);
    }
    public BaseTimeParameter(T value, int parameterIndex, Calendar calendar) {
        this(value,parameterIndex);
        this.calendar = calendar;
    }
    @Override
    public void serialize(TypedSerializer builder) {
        super.serialize(builder);
        builder.write("calendar",calendar);
    }

    @Override
    public Object deserialize(TypedSerializer input) {
        super.deserialize(input);
        this.calendar = input.read("calendar");
        return this;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+"{" +
                "\n\tcalendar=" + calendar +
                ", \n\tvalue=" + value +
                ", \n\tcolumnIndex=" + columnIndex +
                '}';
    }
}
