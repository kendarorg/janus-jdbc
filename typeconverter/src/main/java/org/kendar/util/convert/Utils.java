package org.kendar.util.convert;

import java.sql.Date;
import java.sql.Time;
import java.util.Calendar;

public class Utils {
    public static Date getPureDate(long milliSeconds) {
        final Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(milliSeconds);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    public static Time getPureTime(long milliSeconds) {
        final Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(milliSeconds);
        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, 0);
        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.MILLISECOND, 0);
        return new Time(cal.getTimeInMillis());
    }
}
