package org.kendar.typed.serializer;

public interface TypedSerializable<T> {
    void serialize(TypedSerializer builder);
    T deserialize(TypedSerializer builder);
}
