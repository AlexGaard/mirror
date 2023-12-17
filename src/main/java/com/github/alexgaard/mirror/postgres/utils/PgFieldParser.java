package com.github.alexgaard.mirror.postgres.utils;

import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.core.exception.NotYetImplementedException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static com.github.alexgaard.mirror.postgres.utils.ParseUtils.toByteArray;
import static java.lang.String.format;

public class PgFieldParser {

    private static final DateTimeFormatter localDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final DateTimeFormatter offsetDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSx");

    public static Object parseFieldData(Field.Type fieldType, Object fieldData) {
        if (fieldData == null) {
            return null;
        }

        switch (fieldType) {
            case NULL:
                return null;
            case JSON:
            case STRING:
                if (!(fieldData instanceof String)) {
                    throw new IllegalArgumentException("Data is not a String");
                }

                return fieldData;
            case UUID:
                return UUID.fromString((String) fieldData);
            case FLOAT:
                return Float.valueOf((String) fieldData);
            case DOUBLE:
                return Double.valueOf((String) fieldData);
            case BOOLEAN:
                return "t".equals(fieldData);
            case INT16:
                return Short.parseShort((String) fieldData);
            case INT32:
                return Integer.parseInt((String) fieldData);
            case INT64:
                return Long.parseLong((String) fieldData);
            case CHAR:
                return ((String) fieldData).charAt(0);
            case DATE:
                return LocalDate.parse((String) fieldData);
            case TIME:
                return LocalTime.parse((String) fieldData);
            case TIMESTAMP:
                // TODO: Precision is variable, ex: 2023-12-16 12:32:36.62664
                return LocalDateTime.parse((String) fieldData, localDateTimeFormatter);
            case TIMESTAMP_TZ:
                return OffsetDateTime.parse((String) fieldData, offsetDateTimeFormatter);
            case BYTES:
                String fieldDataStr = (String) fieldData;

                if (!fieldDataStr.startsWith("\\x")) {
                    throw new IllegalArgumentException("Field of type BYTES is not hex encoded");
                }

                return toByteArray(((String) fieldData).substring(2));
            default:
                throw new NotYetImplementedException(format("Parsing for type %s is not yet implemented", fieldType));
        }
    }

}
