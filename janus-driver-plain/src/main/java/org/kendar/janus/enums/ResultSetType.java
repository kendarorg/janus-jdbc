package org.kendar.janus.enums;

import java.util.HashMap;
import java.util.Map;

public enum ResultSetType {
    FORWARD_ONLY(1003),
    SCROLL_INSENSITIVE(1004),
    SCROLL_SENSITIVE(1005);

    private final int value;

    private static final Map<Integer, ResultSetType> typesMap = new HashMap<>();
    static {
        for (ResultSetType type : ResultSetType.values()) {
            typesMap.put(type.value,type);
        }
    }

    ResultSetType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ResultSetType valueOf(int value){
        var type = typesMap.get(value);
        if(type==null) type = ResultSetType.FORWARD_ONLY;
        return type;
    }

    public boolean is(int value){
        return value==this.value;
    }

}
