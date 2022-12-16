package com.toddfast.util.convert.conversion;

import com.toddfast.util.convert.TypeConverter;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Convert to a byte by parsing the value as a string
 *
 */
public class ByteTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			Byte.class,
			Byte.TYPE,
			Byte.class.getName(),
			TypeConverter.TYPE_BYTE
		};
	}

	@Override
	public Object convert(Object value) {
		if (value==null) {
			return null;
		}

		var name = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);
		switch (name){
			case("boolean"):
				return (byte)(((boolean)value) ? 1 : 0);
			case("int"):
			case("integer"):
				return ((Integer)value).byteValue();
			case("byte"):
				return ((Byte)value).byteValue();
			case("short"):
				return ((Short)value).byteValue();
			case("long"):
				return ((Long)value).byteValue();
			case("float"):
				return ((Float)value).byteValue();
			case("double"):
				return ((Double)value).byteValue();
			case("bigdecimal"):
				return ((BigDecimal) value).byteValue();
			case("string"):
				try {
					return Byte.parseByte((String)value);
				}
				catch (NumberFormatException e) {
					throw new RuntimeException("Can't convert String value '" + value + "' to byte");
				}
			default:
				throw new RuntimeException("Can't convert type to boolean: " + value.getClass());
		}
	}
}
