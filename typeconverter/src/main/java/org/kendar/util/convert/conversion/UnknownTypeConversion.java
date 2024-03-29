package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

/**
 * Returns the value as-is (no conversion)
 *
 */
public class UnknownTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] { TypeConverter.TYPE_UNKNOWN };
	}

	@Override
	public Object convert(Object value) {
		return value;
	}
}
