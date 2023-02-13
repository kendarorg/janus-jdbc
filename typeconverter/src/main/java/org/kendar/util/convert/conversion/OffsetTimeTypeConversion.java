package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import static org.apache.commons.lang3.CharSetUtils.count;
import static org.kendar.util.convert.conversion.SqlUtilDate.toSqlDate;
import static org.kendar.util.convert.conversion.StringUtilDate.toMaxDateTime;

/**
 * Convert to a  by parsing a value as a string of
 * form <code>hh:mm:ss</code>.
 *
 * @see	Date#valueOf(String)
 */
public class OffsetTimeTypeConversion implements TypeConverter.Conversion {

	@Override
	public Object[] getTypeKeys() {
		return new Object[] {
			OffsetTime.class,
				OffsetTime.class.getName(),
			TypeConverter.TYPE_OFFSET_TIME
		};
	}

	@Override
	public Object convert(Object value) {
		if (value==null) {
			return null;
		}
		var name = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);
		if(value.getClass()==OffsetTime.class){
			return value;
		}
		switch (name) {
			case("date"): {
				var cal = new GregorianCalendar();
				cal.setTime(toSqlDate(value));
				return  OffsetTime.ofInstant(cal.toInstant(),ZoneId.systemDefault());
			}
			case("localdate"): {
				return OffsetTime.ofInstant(((LocalDate)value).atStartOfDay()
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
				throw new RuntimeException("Can't convert type to offsettime: " + value.getClass());
			}
		}
	}
}
