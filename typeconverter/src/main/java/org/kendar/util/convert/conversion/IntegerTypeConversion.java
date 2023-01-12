package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

import java.math.BigDecimal;
import java.util.Locale;

/**
 * Convert to an integer by parsing the value as a string
 *
 */
public class IntegerTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			Integer.class,
			Integer.TYPE,
			Integer.class.getName(),
			TypeConverter.TYPE_INT,
			TypeConverter.TYPE_INTEGER
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
				return (((boolean)value) ? 1 : 0);
			case("int"):
			case("integer"):
				return ((Integer)value).intValue();
			case("byte"):
				return ((Byte)value).intValue();
			case("short"):
				return ((Short)value).intValue();
			case("long"):
				return ((Long)value).intValue();
			case("float"):
				return ((Float)value).intValue();
			case("double"):
				return ((Double)value).intValue();
			case("bigdecimal"):
				return ((BigDecimal) value).intValue();
			case("string"):
				try {
					return Integer.parseInt((String)value);
				}
				catch (NumberFormatException e) {
					throw new RuntimeException("Can't convert String value '" + value + "' to short");
				}
			default:
				throw new RuntimeException("Can't convert type to boolean: " + value.getClass());
		}
	}
}
