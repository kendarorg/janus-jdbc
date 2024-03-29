package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

/**
 * Converts the value to a string. If the value is a byte or char array,
 * it is converted to a string via {@link toString()}.
 *
 */
public class StringTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			String.class,
			String.class.getName(),
			TypeConverter.TYPE_STRING
		};
	}

	@Override
	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		if (value.getClass().isArray()) {
			// This is a byte array; we can convert it to a string
			if (value.getClass().getComponentType()==Byte.TYPE) {
				value=new String((byte[])value);
			}
			else if (value.getClass().getComponentType()==Character.TYPE) {
				value=new String((char[])value);
			}
		}
		else if (!(value instanceof String)) {
			value=value.toString();
		}
		return value;
	}
}
