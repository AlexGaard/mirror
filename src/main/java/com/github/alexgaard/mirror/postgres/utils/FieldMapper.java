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

public class FieldMapper {

    public static Field<?> mapTupleDataToField(String fieldName, Field.Type fieldType, TupleDataColumn tupleDataColumn) {
        Object parsedData = parseTupleColumnData(fieldType, tupleDataColumn.getData());

        return new Field<>(fieldName, fieldType, parsedData);
    }

    private static Object parseTupleColumnData(Field.Type fieldType, Object fieldData) {
        if (fieldData == null) {
            return null;
        }

        switch (fieldType) {
            case NULL:
                return null;
            case JSON:
            case TEXT:
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
                return DateParser.parseVariablePrecisionLocalDateTime((String) fieldData);
            case TIMESTAMP_TZ:
                return DateParser.parseVariablePrecisionOffsetDateTime((String) fieldData);
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
