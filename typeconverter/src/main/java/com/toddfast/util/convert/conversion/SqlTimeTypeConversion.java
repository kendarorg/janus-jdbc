package com.toddfast.util.convert.conversion;

import com.toddfast.util.convert.TypeConverter;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Locale;

import static com.toddfast.util.convert.Utils.getPureTime;

/**
 * Convert to a {@link SqlTime} by parsing a value as a string of
 * form <code>hh:mm:ss</code>.
 *
 * @see	java.sql.Date#valueOf(String)
 */
public class SqlTimeTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			java.sql.Time.class,
			java.sql.Time.class.getName(),
			TypeConverter.TYPE_SQL_TIME
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
				return getPureTime(((Date)value).getTime());
			}
			case("localdatetime"): {
				return getPureTime(Date.from(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant()).getTime());
			}
			case("localdate"): {
				return getPureTime( Date.from(((LocalDate) value).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()).getTime());
			}

			case("localtime"): {
				return Time.valueOf((LocalTime)value);
			}
			case ("timestamp"): {
				return getPureTime(((Timestamp)value).getTime());
			}
			default: {
				throw new RuntimeException("Can't convert type to time: " + value.getClass());
			}
		}
	}
}
