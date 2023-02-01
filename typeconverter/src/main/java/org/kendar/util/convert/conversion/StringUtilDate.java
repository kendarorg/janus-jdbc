package org.kendar.util.convert.conversion;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.regex.Pattern;

public class StringUtilDate {
    static Pattern hhmm = Pattern.compile("^\\d{2}:\\d{2}.*", Pattern.CASE_INSENSITIVE);
    static Pattern yyyymmdd = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}.*", Pattern.CASE_INSENSITIVE);
    static Pattern numeric = Pattern.compile("^\\d+$", Pattern.CASE_INSENSITIVE);

    public static Object toMaxDateTime(String input) {
        try {
            if (input == null || input.isEmpty()) {
                return null;
            }
            if (numeric.matcher(input).matches()) {
                return new Timestamp(Long.valueOf(input));
            }
            if (yyyymmdd.matcher(input).matches()) {
                var inputs = input.split(" ");
                return LocalDate.parse(inputs[0]);
            }else if (hhmm.matcher(input).matches()){
                var inputs = input.split(" ");
                return LocalTime.parse(inputs[0]);
            }
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }
        throw new RuntimeException("No conversion founded for "+input);
    }
}
