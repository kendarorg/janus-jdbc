package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Converts a byte array to an object via deserialization, or returns the
 * value as-is
 *
 */
public class ObjectTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			Object.class,
			Object.class.getName(),
			TypeConverter.TYPE_OBJECT
		};
	}

	@Override
	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		if (value.getClass().isArray()) {
			// This is a byte array; presume we can convert it to an object
			if (value.getClass().getComponentType()==Byte.TYPE) {
				try (ByteArrayInputStream bis = new ByteArrayInputStream((byte[]) value); ObjectInputStream ois = new ObjectInputStream(bis)) {
					value = ois.readObject();
				} catch (Exception e) {
					throw new IllegalArgumentException(
							"Could not deserialize object", e);
				}
				// Ignore
				// Ignore
			}//else value is OK as is
		}

		return value;
	}
}
