package org.kendar.janus.utils;

import java.util.HashMap;
import java.util.Map;

public enum SqlTypes {
    STRUCT(2002);

    private final int value;

    private static Map<Integer,SqlTypes> typesMap = new HashMap<>();
    static {
        for (SqlTypes type : SqlTypes.values()) {
            typesMap.put(type.value,type);
        }
    }

    SqlTypes(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static SqlTypes valueOf(int value){
        var type = typesMap.get(value);
        if(type==null) type = SqlTypes.STRUCT;
        return type;
    }

    public boolean is(int value){
        return value==this.value;
    }
}
