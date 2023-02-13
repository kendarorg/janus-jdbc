package org.kendar.util.convert.conversion;

import org.kendar.util.convert.TypeConverter;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.Date;
import java.util.regex.Pattern;

public class StringUtilDate {
    static Pattern hhmm = Pattern.compile("^\\d{2}:\\d{2}.*", Pattern.CASE_INSENSITIVE);
    static Pattern yyyymmdd = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}.*", Pattern.CASE_INSENSITIVE);

    static Pattern yyyymmddhhmm = Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}.*", Pattern.CASE_INSENSITIVE);
    static Pattern yyyymmtddhhmm = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}.*", Pattern.CASE_INSENSITIVE);
    static Pattern numeric = Pattern.compile("^\\d+$", Pattern.CASE_INSENSITIVE);

    public static Object toMaxDateTime(String input) {
        try {
            if (input == null || input.isEmpty()) {
                return null;
            }
            if (numeric.matcher(input).matches()) {
                return new Timestamp(Long.valueOf(input));
            }
            if (yyyymmtddhhmm.matcher(input).matches()) {

                try{
                    var real = input.substring(0,10)+" "+input.substring(11);
                    return Timestamp.valueOf(real);
                }catch (Exception ex){
                    return OffsetDateTime.parse(input);
                }
            }else if (yyyymmddhhmm.matcher(input).matches()) {
                try{
                    return Timestamp.valueOf(input);
                }catch (Exception ex){
                    return OffsetDateTime.parse(input);
                }
            }else if (yyyymmdd.matcher(input).matches()) {
                try {
                    var inputs = input.split(" ");
                    return LocalDate.parse(inputs[0]);
                }catch (Exception ex){
                    return OffsetDateTime.parse(input);
                }
            }else if (hhmm.matcher(input).matches()){
                var inputs = input.split(" ");
                try {
                    return LocalTime.parse(inputs[0]);
                }catch (Exception ex){
                    return OffsetTime.parse(input);
                }
            }
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }
        throw new RuntimeException("No conversion founded for "+input);
    }
}
