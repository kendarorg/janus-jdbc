package com.toddfast.util.convert.conversion;

import com.toddfast.util.convert.TypeConverter;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Convert to a float by parsing the value as a string
 *
 */
public class FloatTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			Float.class,
			Float.TYPE,
			Float.class.getName(),
			TypeConverter.TYPE_FLOAT
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
				return (((boolean)value) ? 1.0F : 0.0F);
			case("int"):
			case("integer"):
				return ((Integer)value).floatValue();
			case("byte"):
				return ((Byte)value).floatValue();
			case("short"):
				return ((Short)value).floatValue();
			case("long"):
				return ((Long)value).floatValue();
			case("float"):
				return ((Float)value).floatValue();
			case("double"):
				return ((Double)value).floatValue();
			case("bigdecimal"):
				return ((BigDecimal) value).floatValue();
			case("string"):
				try {
					return Float.parseFloat((String)value);
				}
				catch (NumberFormatException e) {
					throw new RuntimeException("Can't convert String value '" + value + "' to float");
				}
			default:
				throw new RuntimeException("Can't convert type to boolean: " + value.getClass());
		}
	}
}
