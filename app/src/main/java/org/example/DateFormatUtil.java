package org.example;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class DateFormatUtil {
    private static final Map<Integer, String> dateFormat = new HashMap<>();
    static {
        dateFormat.put(8, "HH:mm:ss");
        dateFormat.put(10, "yyyy-MM-dd");
        dateFormat.put(23, "yyyy-MM-dd HH:mm:ss.SSS");
        dateFormat.put(26, "yyyy-MM-dd-HH.mm.ss.SSSSSS");
    }

    public static String getFormatFromLength(int length) {
        return dateFormat.get(length);
    }

    public static Object parseDateStringToDateSql(String dateString) {
        String value = dateString.trim();
        String format = getFormatFromLength(value.length());
        if (format != null && !value.isEmpty()) {
            try {
                if (format.equals("yyyy-MM-dd")) {
                    LocalDate localDate = LocalDate.parse(dateString, DateTimeFormatter.ofPattern(format));
                    return Date.valueOf(localDate);
                } else if (format.equals("HH:mm:ss")) {
                    LocalTime localTime = LocalTime.parse(dateString, DateTimeFormatter.ofPattern(format));
                    return Time.valueOf(localTime);
                } else {
                    LocalDateTime localDateTime = LocalDateTime.parse(value, DateTimeFormatter.ofPattern(format));
                    return Date.valueOf(localDateTime.toLocalDate());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
