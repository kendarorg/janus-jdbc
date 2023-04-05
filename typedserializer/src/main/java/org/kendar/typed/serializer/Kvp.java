package org.kendar.typed.serializer;

public class Kvp {
    public Kvp(){

    }
    private String type;
    private Object value;

    public String getType() {
        return type;
    }

    public Kvp(String type, Object value) {
        this.type = type;
        this.value = value;
    }

    public void setType(String type) {
        this.type = type;
    }
}
