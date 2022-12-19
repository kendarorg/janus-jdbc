package org.kendar.janus.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.lang3.ClassUtils;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JsonTypedSerializer implements TypedSerializer {
    public static final String MAP_KEY_INDEX = "_key";
    public static final String MAP_VALUE_INDEX = "_value";
    public static final String ARRAY_ITEM_INDEX = "_";
    private static ObjectMapper mapper = new ObjectMapper();

    static{
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    private JsonNode root = null;
    private JsonNode currentNode = null;

    @Override
    public void write(String key, Object value) {
        if (key == null) {
            key = ARRAY_ITEM_INDEX;
        }
        if(value==null){
            return;
        }
        key = key.toLowerCase(Locale.ROOT);
        if (ClassUtils.isAssignable(value.getClass(), Collection.class)) {
            writeCollection(key, value);
        } else if (ClassUtils.isAssignable(value.getClass(), Map.class)) {
            writeMap(key, value);
        } else if (value.getClass().isArray()) {
            writeArray(key, value);
        } else if (ClassUtils.isAssignable(value.getClass(), TypedSerializable.class)) {
            writeSerializable(key, value);
        } else if (ClassUtils.isPrimitiveOrWrapper(value.getClass()) || value.getClass() == String.class) {
            writePrimitive(key, value);
        } else {
            if (currentNode instanceof ObjectNode) {

                ((ObjectNode) currentNode).set(key, mapper.valueToTree(value));
                ((ObjectNode) currentNode).put(":" + key + ":", value.getClass().getName());
            } else if (currentNode instanceof ArrayNode) {

            }
        }
    }

    private void writePrimitive(String key, Object value) {
        if (currentNode instanceof ObjectNode) {

            ((ObjectNode) currentNode).set(key, mapper.valueToTree(value));
            ((ObjectNode) currentNode).put(":" + key + ":", value.getClass().getName());
        } else if (currentNode instanceof ArrayNode) {
            var node = mapper.createObjectNode();
            node.set(key, mapper.valueToTree(value));
            node.put(":" + key + ":", value.getClass().getName());
            ((ArrayNode) currentNode).add(node);
        }
    }

    private void writeSerializable(String key, Object value) {
        var objectNode = mapper.createObjectNode();


        if (root == null) {
            root = mapper.createObjectNode();
        }
        if (currentNode == null) {
            currentNode = root;
        }

        ((ObjectNode) currentNode).set(key, objectNode);
        ((ObjectNode) currentNode).put(":" + key + ":", value.getClass().getName());

        var prevCurrent = currentNode;
        currentNode = objectNode;
        ((TypedSerializable) value).serialize(this);
        currentNode = prevCurrent;
    }

    private void writeMap(String key, Object value) {
        var objectNode = mapper.createArrayNode();


        if (root == null) {
            root = mapper.createObjectNode();
        }
        if (currentNode == null) {
            currentNode = root;
        }


        ((ObjectNode) currentNode).set(key, objectNode);
        ((ObjectNode) currentNode).put(":" + key + ":", value.getClass().getName());


        var prevCurrent = currentNode;
        currentNode = objectNode;
        for (var itemKey : ((Map) value).keySet()) {
            var itemNode = mapper.createObjectNode();
            ((ArrayNode) currentNode).add(itemNode);
            var ppc = currentNode;
            currentNode = itemNode;
            write(MAP_KEY_INDEX, itemKey);
            write(MAP_VALUE_INDEX, ((Map) value).get(itemKey));
            currentNode = ppc;
        }
        currentNode = prevCurrent;
    }

    public static Class<?> getArrayClass(String name) throws ClassNotFoundException{
        name = name.replaceAll("\\[","");
        if(name.startsWith("Z")) return boolean.class;
        if(name.startsWith("B")) return byte.class;
        if(name.startsWith("C")) return char.class;
        if(name.startsWith("D")) return double.class;
        if(name.startsWith("F")) return float.class;
        if(name.startsWith("I")) return int.class;
        if(name.startsWith("J")) return long.class;
        if(name.startsWith("S")) return short.class;
        name = name.substring(1,name.length()-1);
        return Class.forName(name);
    }

    @Override
    public <T> T read(String key) {
        if (currentNode == null) {
            currentNode = root;
        }

        try {
            key = key.toLowerCase(Locale.ROOT);
            var item = currentNode.get(key);
            if(item==null){
                return null;
            }
            var itemClassName = currentNode.get(":" + key + ":");
            Class<?> typeClass = Class.forName(itemClassName.textValue());

            if (ClassUtils.isAssignable(typeClass, Collection.class)) {
                return (T)readCollection((ArrayNode) item, typeClass);
            } else if (ClassUtils.isAssignable(typeClass, Map.class)) {
                return (T)readMap((ArrayNode) item, typeClass);
            } else if (typeClass == String.class) {
                return (T)item.textValue();
            } else if (typeClass.isArray()) {
                var splitted = currentNode.get("[" + key + "]").textValue().split(",");
                var ints = new int[splitted.length];
                for (int i = 0; i < splitted.length; i++) {
                    ints[i]=Integer.valueOf(splitted[i]);
                }
                return (T)readArray((ArrayNode) item, itemClassName,ints);
            } else if (ClassUtils.isPrimitiveOrWrapper(typeClass)) {
                return (T)mapper.treeToValue(item, typeClass);
            } else if (ClassUtils.isAssignable(typeClass, TypedSerializable.class)) {
                return (T)readSerializable(item, typeClass);
            } else {
                return (T)mapper.treeToValue(item, typeClass);
            }

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private Object readSerializable(JsonNode item, Class<?> typeClass) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        var serializable = (TypedSerializable) createInstance(typeClass);
        var prevCurrent = currentNode;
        currentNode = item;
        var toret = serializable.deserialize(this);
        currentNode = prevCurrent;
        return toret;
    }

    private Map readMap(ArrayNode item, Class<?> typeClass) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        var result = (Map) createInstance(typeClass);
        var arr = item;
        var prevNode = currentNode;

        for (var i = 0; i < arr.size(); i++) {
            currentNode = arr.get(i);
            var parsedKey = read(MAP_KEY_INDEX);
            var parsedValue = read(MAP_VALUE_INDEX);
            result.put(parsedKey, parsedValue);
        }
        currentNode = prevNode;
        return result;
    }

    private Collection readCollection(ArrayNode item, Class<?> typeClass) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        var result = (Collection) createInstance(typeClass);
        var arr = item;
        var prevNode = currentNode;

        for (var i = 0; i < arr.size(); i++) {
            currentNode = arr.get(i);
            var parsed = read(ARRAY_ITEM_INDEX);
            result.add(parsed);
        }
        currentNode = prevNode;
        return result;
    }
    private static ConcurrentHashMap<Class<?>, Constructor> constructors = new ConcurrentHashMap<>();

    private Object createInstance(Class<?> typeClass) throws InvocationTargetException, InstantiationException, IllegalAccessException {
        if(!constructors.containsKey(typeClass)){
            Constructor[] ctors = typeClass.getDeclaredConstructors();
            for(var ctor:ctors){
                if(ctor.getParameterCount()==0){
                    constructors.put(typeClass,ctor);
                    break;
                }
            }
        }
        var constructor = constructors.get(typeClass);
        if(constructor==null){
            throw new InstantiationException("Missing default constructor in "+typeClass.getSimpleName());
        }
        return constructor.newInstance();
    }

    public TypedSerializer newInstance(Object ... params) {
        return new JsonTypedSerializer();
    }

    @Override
    public Object getSerialized() {
        try {
            return mapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deserialize(Object toDeserialize) {
        try {
            this.root = mapper.readTree((String) toDeserialize);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    private void writeCollection(String key, Object value) {
        var objectNode = mapper.createArrayNode();

        if (root == null) {
            root = mapper.createObjectNode();
        }
        if (currentNode == null) {
            currentNode = root;
        }

        ((ObjectNode) currentNode).set(key, objectNode);
        ((ObjectNode) currentNode).put(":" + key + ":", value.getClass().getName());


        var prevCurrent = currentNode;
        currentNode = objectNode;
        for (var item : ((Collection) value)) {
            var prepre = currentNode;
            currentNode = mapper.createObjectNode();
            ((ArrayNode)prepre).add(currentNode);
            write(null, item);
            currentNode= prepre;
        }
        currentNode = prevCurrent;
    }

    private void writeArray(String key, Object value) {
        var objectNode = mapper.createArrayNode();

        if (root == null) {
            root = mapper.createObjectNode();
        }
        if (currentNode == null) {
            currentNode = root;
        }

        ((ObjectNode) currentNode).set(key, objectNode);

        var dimensions = new ArrayList<String>();
        var prevCurrent = currentNode;
        currentNode = objectNode;
        var length = Array.getLength(value);
        dimensions.add(String.valueOf(length));
        for (var i=0;i<length;i++) {
            var item = Array.get(value,i);

            if(item!=null && item.getClass().isArray() && i==0){
                addDimension(dimensions,item);
            }
            var prepre = currentNode;
            currentNode = mapper.createObjectNode();
            ((ArrayNode)prepre).add(currentNode);
            write(null, item);
            currentNode= prepre;
        }

        var stringDimensions = String.join(",",dimensions);
        currentNode = prevCurrent;
        ((ObjectNode) currentNode).put(":" + key +":", value.getClass().getName());
        ((ObjectNode) currentNode).put("[" + key + "]", stringDimensions);

    }

    private void addDimension(ArrayList<String> dimensions, Object value) {
        var length = Array.getLength(value);
        dimensions.add(String.valueOf(length));
        for (var i=0;i<1;i++) {
            var item = Array.get(value, i);
            if(item!=null && item.getClass().isArray()){
                addDimension(dimensions,item);
            }
        }
    }

    private Object readArray(ArrayNode item, JsonNode itemClassName, int[] dimensions) throws ClassNotFoundException {
        var arr = item;
        var arrayClass = getArrayClass(itemClassName.textValue());

        var result = Array.newInstance(arrayClass,dimensions);
        var prevNode = currentNode;
        for (var i = 0; i < arr.size(); i++) {
            currentNode = arr.get(i);
            var parsed = read(ARRAY_ITEM_INDEX);
            Array.set(result,i,parsed);
        }
        currentNode = prevNode;
        return result;
    }
}
