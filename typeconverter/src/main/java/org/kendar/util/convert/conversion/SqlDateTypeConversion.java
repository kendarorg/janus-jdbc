package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

import java.util.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Locale;

import static org.kendar.util.convert.Utils.getPureDate;

/**
 * Convert to a {@link SqlDate} by parsing a value as a string of
 * form <code>yyyy-[m]m-[d]d</code>.
 *
 * @see	java.sql.Date#valueOf(String)
 */
public class SqlDateTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			java.sql.Date.class,
			java.sql.Date.class.getName(),
			TypeConverter.TYPE_SQL_DATE
		};
	}

	@Override
	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		var name = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);

		Date result;
		switch (name) {
			case("date"): {
				result= (Date)value;
				break;
			}
			case("localdatetime"): {
				result=  Date.from(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant());
				break;
			}
			case("localdate"): {
				result=  Date.from(((LocalDate) value).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
				break;
			}
			case("time"): {
				result=  getPureDate(((Time)value).getTime());
				break;
			}
			case ("timestamp"): {
				result=  getPureDate(((Timestamp)value).getTime());
				break;
			}
			default: {
				throw new RuntimeException("Can't convert type to date: " + value.getClass());
			}
		}
		return new java.sql.Date(result.getTime());
	}
}
