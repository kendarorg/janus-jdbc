package com.toddfast.util.convert.conversion;

import com.toddfast.util.convert.TypeConverter;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Convert to a short by parsing the value as a string
 *
 */
public class ShortTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			Short.class,
			Short.TYPE,
			Short.class.getName(),
			TypeConverter.TYPE_SHORT
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
				return (short)(((boolean)value) ? 1 : 0);
			case("int"):
			case("integer"):
				return ((Integer)value).shortValue();
			case("byte"):
				return ((Byte)value).shortValue();
			case("short"):
				return ((Short)value).shortValue();
			case("long"):
				return ((Long)value).shortValue();
			case("float"):
				return ((Float)value).shortValue();
			case("double"):
				return ((Double)value).shortValue();
			case("bigdecimal"):
				return ((BigDecimal) value).shortValue();
			case("string"):
				try {
					return Short.parseShort((String)value);
				}
				catch (NumberFormatException e) {
					throw new RuntimeException("Can't convert String value '" + value + "' to short");
				}
			default:
				throw new RuntimeException("Can't convert type to boolean: " + value.getClass());
		}
	}
}
