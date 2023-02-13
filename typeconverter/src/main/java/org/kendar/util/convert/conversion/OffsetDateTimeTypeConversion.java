package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.GregorianCalendar;
import java.util.Locale;

import static org.kendar.util.convert.conversion.SqlUtilDate.toSqlDate;
import static org.kendar.util.convert.conversion.StringUtilDate.toMaxDateTime;

/**
 * Convert to a {@link SqlTimestamp} by parsing a value as a string of
 * form <code>yyyy-[m]m-[d]d hh:mm:ss[.f...]</code>.
 *
 * @see	Date#valueOf(String)
 */
public class OffsetDateTimeTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			OffsetDateTime.class,
				OffsetDateTime.class.getName(),
			TypeConverter.TYPE_OFFSET_DATE_TIME
		};
	}


	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		if(value.getClass()==OffsetDateTime.class){
			return value;
		}
		var name = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);

		switch (name) {
			case("date"): {
				var cal = new GregorianCalendar();
				cal.setTime(toSqlDate(value));
				return  OffsetDateTime.ofInstant(cal.toInstant(),ZoneId.systemDefault());
			}
			case("localdate"): {
				return OffsetDateTime.ofInstant(((LocalDate)value).atStartOfDay()
						.toInstant((ZoneOffset) ZoneOffset.systemDefault()),ZoneId.systemDefault());
			}
			case("localtime"): {
				LocalDate today = LocalDate.now();
				return convert(((LocalTime)value).atDate(today));
			}
			case ("long"): {
				return convert(TypeConverter.convert(Timestamp.class,value));
			}
			case ("bigdecimal"): {
				return convert(TypeConverter.convert(Long.class,value));
			}
			case("time"): {
				LocalDate today = LocalDate.now();
				return convert(((Time)value).toLocalTime().atDate(today));
			}
			case ("timestamp"): {
				return convert(((Timestamp)value).toLocalDateTime());
			}
			case ("string"): {
				return convert(toMaxDateTime((String)value));
			}
			default: {
				throw new RuntimeException("Can't convert type to timestamp: " + value.getClass());
			}
		}
	}
}
