package com.toddfast.util.convert.conversion;

import com.toddfast.util.convert.TypeConverter;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.Locale;

/**
 * Convert to a {@link SqlTimestamp} by parsing a value as a string of
 * form <code>yyyy-[m]m-[d]d hh:mm:ss[.f...]</code>.
 *
 * @see	Date#valueOf(String)
 */
public class ZoneDateTimeTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
				ZonedDateTime.class,
				ZonedDateTime.class.getName(),
			TypeConverter.TYPE_ZONED_DATE_TIME
		};
	}

	@Override
	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		return TypeConverter.convert(LocalDateTime.class,value)
				.atZone(ZoneId.systemDefault());
	}
}
