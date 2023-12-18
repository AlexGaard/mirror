package com.github.alexgaard.mirror.postgres.event;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

public class Field<T> {

    public enum Type {
        FLOAT,
        DOUBLE,
        BOOLEAN,
        TEXT,
        JSON,
        UUID,
        CHAR,
        INT16,
        INT32,
        INT64,
        BYTES,
        DATE,
        TIME,
        TIMESTAMP,
        TIMESTAMP_TZ,
        NULL,
    }

    public final java.lang.String name;

    public final Type type;

    public final T value;

    public Field() {
        this.name = null;
        this.type = null;
        this.value = null;
    }

    public Field(java.lang.String name, Type type, T value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public static class Float extends Field<java.lang.Float> {
        public Float(String name, java.lang.Float value) {
            super(name, Type.FLOAT, value);
        }
    }

    public static class Double extends Field<java.lang.Double> {
        public Double(String name, java.lang.Double value) {
            super(name, Type.DOUBLE, value);
        }
    }
    public static class Boolean extends Field<java.lang.Boolean> {
        public Boolean(String name, java.lang.Boolean value) {
            super(name, Type.BOOLEAN, value);
        }
    }
    public static class Text extends Field<java.lang.String> {
        public Text(String name, java.lang.String value) {
            super(name, Type.TEXT, value);
        }
    }
    public static class Json extends Field<java.lang.String> {
        public Json(String name, java.lang.String value) {
            super(name, Type.JSON, value);
        }
    }
    public static class Uuid extends Field<UUID> {
        public Uuid(String name, UUID value) {
            super(name, Type.UUID, value);
        }
    }
    public static class Char extends Field<Character> {
        public Char(String name, char value) {
            super(name, Type.CHAR, value);
        }
    }
    public static class Int16 extends Field<Short> {
        public Int16(String name, short value) {
            super(name, Type.INT16, value);
        }
    }
    public static class Int32 extends Field<Integer> {
        public Int32(String name, int value) {
            super(name, Type.INT32, value);
        }
    }
    public static class Int64 extends Field<Long> {
        public Int64(String name, long value) {
            super(name, Type.INT64, value);
        }
    }
    public static class Bytes extends Field<byte[]> {
        public Bytes(String name, byte[] value) {
            super(name, Type.BYTES, value);
        }
    }
    public static class Date extends Field<LocalDate> {
        public Date(String name, LocalDate value) {
            super(name, Type.DATE, value);
        }
    }
    public static class Time extends Field<LocalTime> {
        public Time(String name, LocalTime value) {
            super(name, Type.TIME, value);
        }
    }
    public static class Timestamp extends Field<LocalDateTime> {
        public Timestamp(String name, LocalDateTime value) {
            super(name, Type.TIMESTAMP, value);
        }
    }
    public static class TimestampTz extends Field<OffsetDateTime> {
        public TimestampTz(String name, OffsetDateTime value) {
            super(name, Type.TIMESTAMP_TZ, value);
        }
    }

    public static class Null extends Field<Object> {
        public Null(String name) {
            super(name, Type.NULL, null);
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
