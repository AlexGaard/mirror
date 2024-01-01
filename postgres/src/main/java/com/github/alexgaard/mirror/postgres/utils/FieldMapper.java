package com.github.alexgaard.mirror.postgres.utils;

import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.core.exception.NotYetImplementedException;
import com.github.alexgaard.mirror.postgres.event.FieldType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.github.alexgaard.mirror.postgres.utils.ParseUtils.toByteArray;
import static java.lang.String.format;

public class FieldMapper {

    public static Field<?> mapTupleDataToField(String fieldName, FieldType fieldType, TupleDataColumn tupleDataColumn) {
        Object fieldData = tupleDataColumn.getData();

        if (fieldData == null) {
            return new Field<>(fieldName, fieldType, null);
        }

        switch (fieldType) {
            case JSONB:
                return Field.jsonbField(fieldName, (String) fieldData);
            case JSON:
                return Field.jsonField(fieldName, (String) fieldData);
            case TEXT:
                return Field.textField(fieldName, (String) fieldData);
            case UUID:
                return Field.uuidField(fieldName, UUID.fromString((String) fieldData));
            case FLOAT:
                return Field.floatField(fieldName, Float.valueOf((String) fieldData));
            case DOUBLE:
                return Field.doubleField(fieldName, Double.valueOf((String) fieldData));
            case BOOLEAN:
                return Field.booleanField(fieldName, "t".equals(fieldData));
            case INT16:
                return Field.int16Field(fieldName, Short.parseShort((String) fieldData));
            case INT32:
                return Field.int32Field(fieldName, Integer.parseInt((String) fieldData));
            case INT64:
                return Field.int64Field(fieldName, Long.parseLong((String) fieldData));
            case CHAR:
                return Field.charField(fieldName, ((String) fieldData).charAt(0));
            case DATE:
                return Field.dateField(fieldName, LocalDate.parse((String) fieldData));
            case TIME:
                return Field.timeField(fieldName, LocalTime.parse((String) fieldData));
            case TIMESTAMP:
                return Field.timestampField(fieldName, DateParser.parseVariablePrecisionLocalDateTime((String) fieldData));
            case TIMESTAMP_TZ:
                return Field.timestampTzField(fieldName, DateParser.parseVariablePrecisionOffsetDateTime((String) fieldData));
            case BYTES: {
                String fieldDataStr = (String) fieldData;

                if (!fieldDataStr.startsWith("\\x")) {
                    throw new IllegalArgumentException("Field of type BYTES is not hex encoded");
                }

                return Field.bytesField(fieldName, toByteArray(((String) fieldData).substring(2)));
            }
            // ARRAY TYPES
            case TEXT_ARRAY: {
                return Field.textArrayField(fieldName, splitPostgresArray((String) fieldData));
            }
            case UUID_ARRAY: {
                List<UUID> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(UUID::fromString)
                        .collect(Collectors.toList());

                return Field.uuidArrayField(fieldName, values);
            }
            case FLOAT_ARRAY: {
                List<Float> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(Float::parseFloat)
                        .collect(Collectors.toList());

                return Field.floatArrayField(fieldName, values);
            }
            case DOUBLE_ARRAY: {
                List<Double> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(Double::parseDouble)
                        .collect(Collectors.toList());

                return Field.doubleArrayField(fieldName, values);
            }
            case BOOLEAN_ARRAY: {
                List<Boolean> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map("t"::equals)
                        .collect(Collectors.toList());

                return Field.booleanArrayField(fieldName, values);
            }
            case INT16_ARRAY: {
                List<Short> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(Short::parseShort)
                        .collect(Collectors.toList());

                return Field.int16ArrayField(fieldName, values);
            }
            case INT32_ARRAY: {
                List<Integer> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());

                return Field.int32ArrayField(fieldName, values);
            }
            case INT64_ARRAY: {
                List<Long> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(Long::parseLong)
                        .collect(Collectors.toList());

                return Field.int64ArrayField(fieldName, values);
            }
            case CHAR_ARRAY: {
                List<Character> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(s -> s.charAt(0))
                        .collect(Collectors.toList());

                return Field.charArrayField(fieldName, values);
            }
            case DATE_ARRAY: {
                List<LocalDate> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(LocalDate::parse)
                        .collect(Collectors.toList());

                return Field.dateArrayField(fieldName, values);
            }
            case TIME_ARRAY: {
                List<LocalTime> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(LocalTime::parse)
                        .collect(Collectors.toList());

                return Field.timeArrayField(fieldName, values);
            }
            case TIMESTAMP_ARRAY: {
                List<LocalDateTime> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(DateParser::parseVariablePrecisionLocalDateTime)
                        .collect(Collectors.toList());

                return Field.timestampArrayField(fieldName, values);
            }
            case TIMESTAMP_TZ_ARRAY: {
                List<OffsetDateTime> values = splitPostgresArray((String) fieldData)
                        .stream()
                        .map(DateParser::parseVariablePrecisionOffsetDateTime)
                        .collect(Collectors.toList());

                return Field.timestampTzArrayField(fieldName, values);
            }
            default:
                throw new NotYetImplementedException(format("Parsing for type %s is not yet implemented", fieldType));
        }
    }


    public static List<String> splitPostgresArray(String pgArray) {
        if (!pgArray.startsWith("{") || !pgArray.endsWith("}")) {
            throw new IllegalArgumentException(pgArray + " is not a valid postgres array");
        }

        List<String> valueList = new ArrayList<>();
        int end = pgArray.length() - 1;
        int startOfValIdx =  1;
        boolean isInsideStr = false;
        boolean isStringEscaped = false;

        for (int i = startOfValIdx; i < end; i++) {
            char c = pgArray.charAt(i);

            if (c == '"' && (pgArray.charAt(i - 1) != '\\') ) {
                isInsideStr = !isInsideStr;

                if (isInsideStr) {
                    isStringEscaped = true;
                }
            }

            if (c == ',' && !isInsideStr) {
                String value = isStringEscaped
                        ? pgArray.substring(startOfValIdx + 1, i - 1)
                        : pgArray.substring(startOfValIdx, i);

                valueList.add(replaceEscapedQuote(value));

                isStringEscaped = false;
                startOfValIdx = i + 1;
            }

            if (i + 1 >= end) {
                String value = isStringEscaped
                        ? pgArray.substring(startOfValIdx + 1, end - 1)
                        : pgArray.substring(startOfValIdx, end);

                valueList.add(replaceEscapedQuote(value));
            }
        }

        return valueList;
    }

    // Replaces \" with "
    private static String replaceEscapedQuote(String str) {
        return str.replaceAll("\\\\\"", "\"");
    }

}
