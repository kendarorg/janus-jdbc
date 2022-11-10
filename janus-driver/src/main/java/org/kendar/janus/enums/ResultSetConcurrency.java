package org.kendar.janus.enums;

import java.util.HashMap;
import java.util.Map;

public enum ResultSetConcurrency {
    CONCUR_READ_ONLY(1007),
    CONCUR_UPDATABLE(1008);

    private final int value;

    private static Map<Integer, ResultSetConcurrency> typesMap = new HashMap<>();
    static {
        for (ResultSetConcurrency type : ResultSetConcurrency.values()) {
            typesMap.put(type.value,type);
        }
    }

    ResultSetConcurrency(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ResultSetConcurrency valueOf(int value){
        var type = typesMap.get(value);
        if(type==null) type = ResultSetConcurrency.CONCUR_UPDATABLE;
        return type;
    }

    public boolean is(int value){
        return value==this.value;
    }
}
