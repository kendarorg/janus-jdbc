package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

/**
 * Returns the value as-is (no conversion)
 *
 */
public class IdentityTypeConversion 
		implements TypeConverter.Conversion<Object> {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {};
	}

	@Override
	public Object convert(Object value) {
		return value;
	}
}
