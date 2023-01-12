package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;

/**
 * Convert to a {@link BigDecimal} by parsing the value as a string
 *
 */
public class BigDecimalTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			BigDecimal.class,
			BigDecimal.class.getName(),
			TypeConverter.TYPE_BIG_DECIMAL
		};
	}

	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		var name = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);
		BigDecimal result;
		switch (name){
			case("boolean"):
				result = new BigDecimal(((boolean)value) ? 1.0 : 0.0);
				break;
			case("int"):
			case("integer"):
				result= new BigDecimal((int)value);
				break;
			case("byte"):
				result= new BigDecimal((byte)value);
				break;
			case("short"):
				result= new BigDecimal((short)value);
				break;
			case("long"):
				result= new BigDecimal((long)value);
				break;
			case("float"):
				result= new BigDecimal((float)value);
				break;
			case("double"):
				result= new BigDecimal((double)value);
				break;
			case("bigdecimal"):
				result = (BigDecimal) value;
				break;
			case("string"):
				try {
					result = new BigDecimal((String)value);
				}
				catch (NumberFormatException e) {
					throw new RuntimeException("Can't convert String value '" + value + "' to bigdecimal");
				}
				break;
			default:
				throw new RuntimeException("Can't convert type to boolean: " + value.getClass());
		}
		return  result.setScale(0, RoundingMode.HALF_UP);
	}
}
