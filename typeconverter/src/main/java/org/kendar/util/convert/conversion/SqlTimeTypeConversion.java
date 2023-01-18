package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.Locale;

import static org.kendar.util.convert.Utils.getPureTime;
import static org.kendar.util.convert.conversion.SqlUtilDate.toSqlDate;
import static org.kendar.util.convert.conversion.StringUtilDate.toMaxDateTime;

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
				return getPureTime(toSqlDate(value).getTime());
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
			case ("offsettime"): {
				return Time.valueOf(((OffsetTime)value).toLocalTime());
			}
			case ("string"): {
				return convert(toMaxDateTime((String)value));
			}
			default: {
				throw new RuntimeException("Can't convert type to time: " + value.getClass());
			}
		}
	}
}
