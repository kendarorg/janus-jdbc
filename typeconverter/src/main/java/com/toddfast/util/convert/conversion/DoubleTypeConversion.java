package com.toddfast.util.convert.conversion;

import com.toddfast.util.convert.TypeConverter;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Convert to a double by parsing the value as a string
 *
 */
public class DoubleTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			Double.class,
			Double.TYPE,
			Double.class.getName(),
			TypeConverter.TYPE_DOUBLE
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
				return (((boolean)value) ? 1.0 : 0.0);
			case("int"):
			case("integer"):
				return ((Integer)value).doubleValue();
			case("byte"):
				return ((Byte)value).doubleValue();
			case("short"):
				return ((Short)value).doubleValue();
			case("long"):
				return ((Long)value).doubleValue();
			case("float"):
				return ((Float)value).doubleValue();
			case("double"):
				return ((Double)value).doubleValue();
			case("bigdecimal"):
				return ((BigDecimal) value).doubleValue();
			case("string"):
				try {
					return Double.parseDouble((String)value);
				}
				catch (NumberFormatException e) {
					throw new RuntimeException("Can't convert String value '" + value + "' to double");
				}
			default:
				throw new RuntimeException("Can't convert type to boolean: " + value.getClass());
		}
	}
}
