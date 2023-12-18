package com.github.alexgaard.mirror.postgres.utils;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class DateParser {

    private static final List<DateTimeFormatter> localDateFormatters = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
    );

    private static final List<DateTimeFormatter> offsetDateTimeFormatters = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSSX")
    );

    public static LocalDateTime parseVariablePrecisionLocalDateTime(String localDateTimeStr) {
        int subSecondPrecision = localDateTimeStr.length() - localDateTimeStr.lastIndexOf(".") - 1;

        // Clamp between 1 and 9
        subSecondPrecision = Math.max(1, subSecondPrecision);
        subSecondPrecision = Math.min(9, subSecondPrecision);

        DateTimeFormatter dateTimeFormatter = localDateFormatters.get(subSecondPrecision - 1);

        return LocalDateTime.parse(localDateTimeStr, dateTimeFormatter);
    }

    public static OffsetDateTime parseVariablePrecisionOffsetDateTime(String offsetDateTimeStr) {
        int subSecondIdx = offsetDateTimeStr.lastIndexOf(".");
        int subSecondEndIdx = subSecondIdx;

        for (int i = subSecondIdx + 1; i < offsetDateTimeStr.length(); i++) {
            if (!Character.isDigit(offsetDateTimeStr.charAt(i))) {
                subSecondEndIdx = i;
                break;
            }
        }

        int subSecondPrecision = subSecondEndIdx - subSecondIdx - 1;

        // Clamp between 1 and 9
        subSecondPrecision = Math.max(1, subSecondPrecision);
        subSecondPrecision = Math.min(9, subSecondPrecision);

        DateTimeFormatter dateTimeFormatter = offsetDateTimeFormatters.get(subSecondPrecision - 1);

        return OffsetDateTime.parse(offsetDateTimeStr, dateTimeFormatter);
    }


}
