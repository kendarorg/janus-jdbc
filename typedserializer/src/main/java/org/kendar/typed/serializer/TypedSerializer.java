package org.kendar.typed.serializer;

public interface TypedSerializer {
    void write(String key, Object value);

    <T> T read(String key);
    TypedSerializer newInstance();

    Object getSerialized();
    Object getSimpleSerialized();

    void deserialize(Object toDeserialize);
}
