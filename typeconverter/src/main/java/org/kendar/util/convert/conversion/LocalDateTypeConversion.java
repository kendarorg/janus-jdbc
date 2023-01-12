package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Locale;

/**
 * Convert to a {@link SqlTimestamp} by parsing a value as a string of
 * form <code>yyyy-[m]m-[d]d hh:mm:ss[.f...]</code>.
 *
 * @see	Date#valueOf(String)
 */
public class LocalDateTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			LocalDate.class,
				LocalDate.class.getName(),
			TypeConverter.TYPE_LOCAL_DATE
		};
	}


	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		var name = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);

		switch (name) {
			case("date"): {
					return ((Date) value).toLocalDate();
			}
			case("localdatetime"): {
				return ((LocalDateTime)value).toLocalDate();
			}
			case("localtime"): {
				LocalDate today = LocalDate.now();
				return ((LocalTime)value).atDate(today).toLocalDate();
			}
			case("time"): {
				LocalDate today = LocalDate.now();
				return ((Time)value).toLocalTime().atDate(today).toLocalDate();
			}
			case ("timestamp"): {
				return ((Timestamp)value).toLocalDateTime().toLocalDate();
			}
			default: {
				throw new RuntimeException("Can't convert type to timestamp: " + value.getClass());
			}
		}
	}
}
