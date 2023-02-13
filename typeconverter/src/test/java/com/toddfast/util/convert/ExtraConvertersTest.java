package com.toddfast.util.convert;

import org.junit.jupiter.api.Test;
import org.kendar.util.convert.TypeConverter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExtraConvertersTest {
    @Test
    public void testGeneral() {

        Object in;
        Object out;

        in="12";
        out= TypeConverter.convert(Integer.class,in);
        assertTrue(out instanceof Integer);
        assertEquals(12,out);
    }

}
