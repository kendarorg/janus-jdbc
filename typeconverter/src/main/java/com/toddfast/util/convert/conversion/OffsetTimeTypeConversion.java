package com.toddfast.util.convert.conversion;

import com.toddfast.util.convert.TypeConverter;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.Locale;

import static com.toddfast.util.convert.Utils.getPureTime;

/**
 * Convert to a {@link SqlTime} by parsing a value as a string of
 * form <code>hh:mm:ss</code>.
 *
 * @see	Date#valueOf(String)
 */
public class OffsetTimeTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			OffsetTime.class,
				OffsetTime.class.getName(),
			TypeConverter.TYPE_OFFSET_TIME
		};
	}

	@Override
	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(Instant.now());
		return OffsetTime.of(TypeConverter.convert(LocalTime.class,value),zoneOffset);
	}
}
