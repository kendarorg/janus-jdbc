package org.kendar.janus.results;

import org.kendar.janus.serialization.TypedSerializer;

public class ObjectResult implements JdbcResult {
    public ObjectResult(Object result) {
        this.result = result;
    }

    public ObjectResult(){

    }
    private Object result;
    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("result",result);
    }

    @Override
    public JdbcResult deserialize(TypedSerializer input) {
        try {
            return new ObjectResult(
                    input.read("result")
            );
        }catch (Exception ex){
            return null;
        }
    }

    public <T> T getResult() {
        return (T)result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

}
