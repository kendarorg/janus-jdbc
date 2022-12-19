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
public class LocalDateTimeTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			LocalDateTime.class,
				LocalDateTime.class.getName(),
			TypeConverter.TYPE_LOCAL_DATE_TIME
		};
	}


	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		var name = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);

		switch (name) {
			case("date"): {
				return ((Date)value).toInstant()
						.atZone(ZoneId.systemDefault())
						.toLocalDateTime();
			}
			case("localdate"): {
				return ((LocalDate)value).atStartOfDay();
			}
			case("localtime"): {
				LocalDate today = LocalDate.now();
				return ((LocalTime)value).atDate(today);
			}
			case("time"): {
				LocalDate today = LocalDate.now();
				return ((Time)value).toLocalTime().atDate(today);
			}
			case ("timestamp"): {
				return ((Timestamp)value).toLocalDateTime();
			}
			case ("offsetdatetime"): {
				return ((OffsetDateTime)value).atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
			}
			default: {
				throw new RuntimeException("Can't convert type to timestamp: " + value.getClass());
			}
		}
	}
}
