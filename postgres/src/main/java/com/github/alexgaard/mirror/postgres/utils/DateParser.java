package com.github.alexgaard.mirror.postgres.utils;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class DateParser {

    private static final List<DateTimeFormatter> localDateFormatters = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
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
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX"),
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
        int subSecondPrecisionStart = localDateTimeStr.lastIndexOf(".");

        int subSecondPrecision = subSecondPrecisionStart != -1
                ? localDateTimeStr.length() - localDateTimeStr.lastIndexOf(".") - 1
                : 0;

        subSecondPrecision = clamp(subSecondPrecision, 0, 9);

        DateTimeFormatter dateTimeFormatter = localDateFormatters.get(subSecondPrecision);

        return LocalDateTime.parse(localDateTimeStr, dateTimeFormatter);
    }

    public static OffsetDateTime parseVariablePrecisionOffsetDateTime(String offsetDateTimeStr) {
        int subSecondPrecision = findSubSecondPrecisionForOffsetDateTime(offsetDateTimeStr);

        DateTimeFormatter dateTimeFormatter = offsetDateTimeFormatters.get(subSecondPrecision);

        return OffsetDateTime.parse(offsetDateTimeStr, dateTimeFormatter);
    }

    private static int findSubSecondPrecisionForOffsetDateTime(String offsetDateTimeStr) {
        int subSecondPrecisionStart = offsetDateTimeStr.lastIndexOf(".");

        if (subSecondPrecisionStart == -1) {
            return 0;
        }

        int subSecondEndIdx = subSecondPrecisionStart;

        for (int i = subSecondPrecisionStart + 1; i < offsetDateTimeStr.length(); i++) {
            if (!Character.isDigit(offsetDateTimeStr.charAt(i))) {
                subSecondEndIdx = i;
                break;
            }
        }

        int subSecondPrecision = subSecondEndIdx - subSecondPrecisionStart - 1;

        return clamp(subSecondPrecision, 0, 9);
    }

    private static int clamp(int val, int from, int to) {
        if (val >= to) {
            return to;
        } else if (val <= from) {
            return from;
        }

        return val;
    }

}
