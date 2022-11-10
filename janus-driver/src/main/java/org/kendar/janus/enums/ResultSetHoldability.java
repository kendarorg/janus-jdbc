package org.kendar.janus.enums;

import java.util.HashMap;
import java.util.Map;

public enum ResultSetHoldability {
    DEFAULT(0),
    HOLD_CURSORS_OVER_COMMIT(1),
    CLOSE_CURSORS_AT_COMMIT(2);

    private final int value;

    private static Map<Integer, ResultSetHoldability> typesMap = new HashMap<>();
    static {
        for (ResultSetHoldability type : ResultSetHoldability.values()) {
            typesMap.put(type.value,type);
        }
    }

    ResultSetHoldability(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ResultSetHoldability valueOf(int value){
        var type = typesMap.get(value);
        if(type==null) type = ResultSetHoldability.DEFAULT;
        return type;
    }

    public boolean is(int value){
        return value==this.value;
    }
}
