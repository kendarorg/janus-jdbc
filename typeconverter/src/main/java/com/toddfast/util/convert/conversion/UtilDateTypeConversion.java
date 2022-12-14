package com.toddfast.util.convert.conversion;

import com.toddfast.util.convert.TypeConverter;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.Locale;

import static com.toddfast.util.convert.Utils.getPureDate;

/**
 * Convert to a {@link SqlDate} by parsing a value as a string of
 * form <code>yyyy-[m]m-[d]d</code>.
 *
 * @see	java.sql.Date#valueOf(String)
 */
@SuppressWarnings("ALL")
public class UtilDateTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			java.util.Date.class,
			java.util.Date.class.getName(),
			TypeConverter.TYPE_SQL_DATE
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
				return (Date)value;
			}
			case("localdatetime"): {
				return Date.from(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant());
			}
			case("localdate"): {
				return Date.from(((LocalDate) value).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
			}
			case("time"): {
				return getPureDate(((Time)value).getTime());
			}
			case("localtime"): {

				LocalDate today = LocalDate.now();
				return Date.from(((LocalTime)value).atDate(today).atZone(ZoneId.systemDefault()).toInstant());
			}
			case ("timestamp"): {
				return getPureDate(((Timestamp)value).getTime());
			}
			default: {
				throw new RuntimeException("Can't convert type to date: " + value.getClass());
			}
		}
	}
}
