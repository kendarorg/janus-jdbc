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
 * Convert to a {@link } by parsing a value as a string of
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
		if(value.getClass()==ZonedDateTime.class){
			return value;
		}
		var name = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);

		ZoneId systemZone = ZoneId.systemDefault();
		switch (name) {
			case("date"): {
				var cal = new GregorianCalendar();
				cal.setTime(toSqlDate(value));
				return  ZonedDateTime.ofInstant(cal.toInstant(),ZoneId.systemDefault());
			}
			case("localdate"): {
				return ZonedDateTime.ofInstant(((LocalDate)value).atStartOfDay()
						.toInstant((ZoneOffset) ZoneOffset.systemDefault()),ZoneId.systemDefault());
			}
			case("localtime"): {
				LocalDate today = LocalDate.now();
				return convert(((LocalTime)value).atDate(today));
			}
			case("localdatetime"): {
				return ZonedDateTime.of((LocalDateTime) value, ZoneId.systemDefault());
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
			case ("offsettime"): {
				return convert(((OffsetTime)value).toLocalTime());
			}
			case ("offsetdatetime"): {
				return convert(((OffsetDateTime)value).toLocalDateTime());
			}
			case ("string"): {
				return convert(toMaxDateTime((String)value));
			}
			default: {
				throw new RuntimeException("Can't convert type to zonedatetime: " + value.getClass());
			}
		}
	}
}
