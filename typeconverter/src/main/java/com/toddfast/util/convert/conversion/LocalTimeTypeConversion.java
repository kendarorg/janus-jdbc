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
public class LocalTimeTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			LocalTime.class,
				LocalTime.class.getName(),
			TypeConverter.TYPE_LOCAL_TIME
		};
	}

	@Override
	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		var name = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);

		switch (name) {
			case("date"): {
				return ((Date)value).toInstant()
						.atZone(ZoneId.systemDefault())
						.toLocalTime();
			}
			case("localdate"): {
				return ((LocalDate)value).atStartOfDay().toLocalTime();
			}
			case("localdatetime"): {
				return ((LocalDateTime)value).toLocalTime();
			}
			case("time"): {
				return ((Time)value).toLocalTime();
			}
			case ("timestamp"): {
				return ((Timestamp)value).toLocalDateTime().toLocalTime();
			}
			case ("zoneddatetime"): {
				return ((ZonedDateTime)value).toLocalDateTime().toLocalTime();
			}
			default: {
				throw new RuntimeException("Can't convert type to timestamp: " + value.getClass());
			}
		}
	}
}
