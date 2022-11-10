package org.kendar.janus.cmd;

import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;
import org.kendar.janus.utils.ExceptionsWrapper;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;

public class Exec implements JdbcCommand {
    private static final Object[] EMPTY_PARAMS = new Object[]{};
    private static final Class<?>[] EMPTY_TYPES = new Class<?>[]{};
    private String name;
    private Class<?>[] paramType;
    private Object[] parameters;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Exec(){
        this.paramType = EMPTY_TYPES;
        this.parameters = EMPTY_PARAMS;
    }

    public Exec(String name) {
        this.name = name;
        this.paramType = EMPTY_TYPES;
        this.parameters = EMPTY_PARAMS;
    }

    public Exec withTypes(Class<?> ...types){
        this.paramType=types;
        return this;
    }

    public Exec withParameters(Object ...values){
        this.parameters=values;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Exec exec = (Exec) o;
        return name.equals(exec.name) && Arrays.equals(paramType, exec.paramType) && Arrays.equals(parameters, exec.parameters);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name);
        result = 31 * result + Arrays.hashCode(paramType);
        result = 31 * result + Arrays.hashCode(parameters);
        return result;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("name",name);
        builder.write("paramType",paramType);
        builder.write("parameters",parameters);
    }

    @Override
    public JdbcCommand deserialize(TypedSerializer input) {
        return new Exec(
                input.read("name"))
                .withTypes(input.read("paramType"))
                .withParameters(input.read("parameters"));
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        try {
            var target = context.get(uid);
            var method = target.getClass().getMethod(getName(),
                    paramType);
            Object result =null;
            if(parameters==null) {
                result = method.invoke(target);
            }else{
                result = method.invoke(target,parameters);
            }
            return result;
        }catch (Exception ex){
            throw ExceptionsWrapper.toSQLException(ex);
        }
    }
}
