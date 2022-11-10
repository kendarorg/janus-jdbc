package org.kendar.janus.serialization;

public interface TypedSerializable<T> {
    void serialize(TypedSerializer builder);
    T deserialize(TypedSerializer builder);
}
