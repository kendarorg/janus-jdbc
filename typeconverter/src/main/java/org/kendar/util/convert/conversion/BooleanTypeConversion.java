package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

import java.math.BigDecimal;
import java.util.Locale;

/**
 * Convert to a boolean by parsing the value as a string
 *
 */
public class BooleanTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			Boolean.class,
			Boolean.TYPE,
			Boolean.class.getName(),
			TypeConverter.TYPE_BOOLEAN
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
				return value;
			case("int"):
			case("integer"):
				return ((int) value)!=0;
			case("byte"):
				return ((byte) value)!=0;
			case("short"):
				return ((short) value)!=0;
			case("long"):
				return ((long) value)!=0L;
			case("float"):
				return ((float) value)!=0.0F;
			case("double"):
				return ((double) value)!=0.0;
			case("bigdecimal"):
				return ((BigDecimal) value).intValue()!=0;
			case("string"):
				try {
					return Boolean.parseBoolean((String)value);
				}
				catch (NumberFormatException e) {
					throw new RuntimeException("Can't convert String value '" + value + "' to boolean");
				}
			default:
				throw new RuntimeException("Can't convert type to boolean: " + value.getClass());
		}
	}
}
