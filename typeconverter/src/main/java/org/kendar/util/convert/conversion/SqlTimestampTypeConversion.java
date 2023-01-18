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
 * @see	java.sql.Date#valueOf(String)
 */
public class SqlTimestampTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			java.sql.Timestamp.class,
			java.sql.Timestamp.class.getName(),
			TypeConverter.TYPE_SQL_TIMESTAMP
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
				if(value instanceof java.util.Date){
					return new Timestamp(((java.util.Date) value).getTime());
				}
				return new Timestamp(((Date) value).getTime());
			}
			case("localdatetime"): {
				return Timestamp.valueOf((LocalDateTime) value);
			}
			case("localdate"): {
				return Timestamp.valueOf(((LocalDate) value).atStartOfDay());
			}
			case("localtime"): {
				LocalDate today = LocalDate.now();
				return Timestamp.valueOf(((LocalTime) value).atDate(today));
			}
			case("time"): {
				return new Timestamp(((Time)value).getTime());
			}
			case ("timestamp"): {
				return value;
			}
			default: {
				throw new RuntimeException("Can't convert type to timestamp: " + value.getClass());
			}
		}
	}
}
