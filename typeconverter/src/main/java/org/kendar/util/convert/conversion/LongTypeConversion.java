package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Locale;

/**
 * Convert to a long by parsing the value as a string
 *
 */
public class LongTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			Long.class,
			Long.TYPE,
			Long.class.getName(),
			TypeConverter.TYPE_LONG
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
				return (((boolean)value) ? 1L : 0L);
			case("int"):
			case("integer"):
				return ((Integer)value).longValue();
			case("byte"):
				return ((Byte)value).longValue();
			case("short"):
				return ((Short)value).longValue();
			case("biginteger"):
				return ((BigInteger)value).longValue();
			case("long"):
				return ((Long)value).longValue();
			case("float"):
				return ((Float)value).longValue();
			case("double"):
				return ((Double)value).longValue();
			case("bigdecimal"):
				return ((BigDecimal) value).longValue();
			case("timestamp"):
				return ((Timestamp) value).getTime();
			case("string"):
				try {
					return Long.parseLong((String)value);
				}
				catch (NumberFormatException e) {
					throw new RuntimeException("Can't convert String value '" + value + "' to long");
				}
			default:
				throw new RuntimeException("Can't convert type to boolean: " + value.getClass());
		}
	}
}
