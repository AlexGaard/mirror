package com.github.alexgaard.mirror.postgres.event;

import com.github.alexgaard.mirror.core.exception.NotYetImplementedException;

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static java.lang.String.format;

public class Field<T> {

    public final String name;

    public final FieldType type;

    public final T value;

    public Field() {
        name = null;
        type = null;
        value = null;
    }

    public Field(String name, FieldType type, T value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }
    
    public static Field<Float> floatField(String name, Float value) {
        return new Field<>(name, FieldType.FLOAT, value);
    }

    public static Field<Double> doubleField(String name, Double value) {
        return new Field<>(name, FieldType.DOUBLE, value);
    }

    public static Field<Boolean> booleanField(String name, Boolean value) {
        return new Field<>(name, FieldType.BOOLEAN, value);
    }

    public static Field<String> textField(String name, String value) {
        return new Field<>(name, FieldType.TEXT, value);
    }

    public static Field<String> jsonField(String name, String value) {
        return new Field<>(name, FieldType.JSON, value);
    }

    public static Field<String> jsonbField(String name, String value) {
        return new Field<>(name, FieldType.JSONB, value);
    }

    public static Field<UUID> uuidField(String name, UUID value) {
        return new Field<>(name, FieldType.UUID, value);
    }

    public static Field<Character> charField(String name, Character value) {
        return new Field<>(name, FieldType.CHAR, value);
    }

    public static Field<Short> int16Field(String name, Short value) {
        return new Field<>(name, FieldType.INT16, value);
    }

    public static Field<Integer> int32Field(String name, Integer value) {
        return new Field<>(name, FieldType.INT32, value);
    }

    public static Field<Long> int64Field(String name, Long value) {
        return new Field<>(name, FieldType.INT64, value);
    }

    public static Field<byte[]> bytesField(String name, byte[] value) {
        return new Field<>(name, FieldType.BYTES, value);
    }

    public static Field<LocalDate> dateField(String name, LocalDate value) {
        return new Field<>(name, FieldType.DATE, value);
    }

    public static Field<LocalTime> timeField(String name, LocalTime value) {
        return new Field<>(name, FieldType.TIME, value);
    }

    public static Field<LocalDateTime> timestampField(String name, LocalDateTime value) {
        return new Field<>(name, FieldType.TIMESTAMP, value);
    }

    public static Field<OffsetDateTime> timestampTzField(String name, OffsetDateTime value) {
        return new Field<>(name, FieldType.TIMESTAMP_TZ, value);
    }

    // Array types

    public static Field<List<Float>> floatArrayField(String name, List<Float> value) {
        return new Field<>(name, FieldType.FLOAT_ARRAY, value);
    }

    public static Field<List<Double>> doubleArrayField(String name, List<Double> value) {
        return new Field<>(name, FieldType.DOUBLE_ARRAY, value);
    }

    public static Field<List<Boolean>> booleanArrayField(String name, List<Boolean> value) {
        return new Field<>(name, FieldType.BOOLEAN_ARRAY, value);
    }

    public static Field<List<String>> textArrayField(String name, List<String> value) {
        return new Field<>(name, FieldType.TEXT_ARRAY, value);
    }

    public static Field<List<UUID>> uuidArrayField(String name, List<UUID> value) {
        return new Field<>(name, FieldType.UUID_ARRAY, value);
    }

    public static Field<List<Character>> charArrayField(String name, List<Character> value) {
        return new Field<>(name, FieldType.CHAR_ARRAY, value);
    }

    public static Field<List<Short>> int16ArrayField(String name, List<Short> value) {
        return new Field<>(name, FieldType.INT16_ARRAY, value);
    }

    public static Field<List<Integer>> int32ArrayField(String name, List<Integer> value) {
        return new Field<>(name, FieldType.INT32_ARRAY, value);
    }

    public static Field<List<Long>> int64ArrayField(String name, List<Long> value) {
        return new Field<>(name, FieldType.INT64_ARRAY, value);
    }

    public static Field<List<LocalDate>> dateArrayField(String name, List<LocalDate> value) {
        return new Field<>(name, FieldType.DATE_ARRAY, value);
    }

    public static Field<List<LocalTime>> timeArrayField(String name, List<LocalTime> value) {
        return new Field<>(name, FieldType.TIME_ARRAY, value);
    }

    public static Field<List<LocalDateTime>> timestampArrayField(String name, List<LocalDateTime> value) {
        return new Field<>(name, FieldType.TIMESTAMP_ARRAY, value);
    }

    public static Field<List<OffsetDateTime>> timestampTzArrayField(String name, List<OffsetDateTime> value) {
        return new Field<>(name, FieldType.TIMESTAMP_TZ_ARRAY, value);
    }

    public int toSqlFieldType() {
        if (value == null) {
            return Types.NULL;
        }

        if (type.isArray()) {
            return Types.ARRAY;
        }

        switch (type) {
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case INT16:
                return Types.SMALLINT;
            case INT32:
                return Types.INTEGER;
            case INT64:
                return Types.BIGINT;
            case CHAR:
                return Types.CHAR;
            case BOOLEAN:
                return Types.BOOLEAN;
            case TEXT:
            case JSON:
            case JSONB:
            case UUID:
                return Types.VARCHAR;
            case BYTES:
                return Types.BINARY;
            case DATE:
                return Types.DATE;
            case TIME:
                return Types.TIME;
            case TIMESTAMP:
                return Types.TIMESTAMP;
            case TIMESTAMP_TZ:
                return Types.TIMESTAMP_WITH_TIMEZONE;
            default:
                throw new NotYetImplementedException(format("No mapping to SQL type for field '%s' with type '%s'", name, type));
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field<?> field = (Field<?>) o;
        return Objects.equals(name, field.name) && type == field.type && Objects.equals(value, field.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, value);
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", value=" + value +
                '}';
    }
}
