package com.github.alexgaard.mirror.common_test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class DataArrayTypesDbo {

    public Integer id;
    public Short[] int2_array_field;
    public Integer[] int4_array_field;
    public Long[] int8_array_field;
    public Float[] float4_array_field;
    public Double[] float8_array_field;
    public UUID[] uuid_array_field;
    public String[] varchar_array_field;
    public String[] text_array_field;
    public Boolean[] bool_array_field;
    public Character[] char_array_field;
    public LocalDate[] date_array_field;

    public LocalTime[] time_array_field;

    public LocalDateTime[] timestamp_array_field;
    public OffsetDateTime[] timestamptz_array_field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataArrayTypesDbo that = (DataArrayTypesDbo) o;

        if (!Objects.equals(id, that.id)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(int2_array_field, that.int2_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(int4_array_field, that.int4_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(int8_array_field, that.int8_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(float4_array_field, that.float4_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(float8_array_field, that.float8_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(uuid_array_field, that.uuid_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(varchar_array_field, that.varchar_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(text_array_field, that.text_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(bool_array_field, that.bool_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(char_array_field, that.char_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(date_array_field, that.date_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(time_array_field, that.time_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(timestamp_array_field, that.timestamp_array_field)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(timestamptz_array_field, that.timestamptz_array_field);
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(int2_array_field);
        result = 31 * result + Arrays.hashCode(int4_array_field);
        result = 31 * result + Arrays.hashCode(int8_array_field);
        result = 31 * result + Arrays.hashCode(float4_array_field);
        result = 31 * result + Arrays.hashCode(float8_array_field);
        result = 31 * result + Arrays.hashCode(uuid_array_field);
        result = 31 * result + Arrays.hashCode(varchar_array_field);
        result = 31 * result + Arrays.hashCode(text_array_field);
        result = 31 * result + Arrays.hashCode(bool_array_field);
        result = 31 * result + Arrays.hashCode(char_array_field);
        result = 31 * result + Arrays.hashCode(date_array_field);
        result = 31 * result + Arrays.hashCode(time_array_field);
        result = 31 * result + Arrays.hashCode(timestamp_array_field);
        result = 31 * result + Arrays.hashCode(timestamptz_array_field);
        return result;
    }

    @Override
    public String toString() {
        return "DataArrayTypesDbo{" +
                "id=" + id +
                ", int2_array_field=" + Arrays.toString(int2_array_field) +
                ", int4_array_field=" + Arrays.toString(int4_array_field) +
                ", int8_array_field=" + Arrays.toString(int8_array_field) +
                ", float4_array_field=" + Arrays.toString(float4_array_field) +
                ", float8_array_field=" + Arrays.toString(float8_array_field) +
                ", uuid_array_field=" + Arrays.toString(uuid_array_field) +
                ", varchar_array_field=" + Arrays.toString(varchar_array_field) +
                ", text_array_field=" + Arrays.toString(text_array_field) +
                ", bool_array_field=" + Arrays.toString(bool_array_field) +
                ", char_array_field=" + Arrays.toString(char_array_field) +
                ", date_array_field=" + Arrays.toString(date_array_field) +
                ", time_array_field=" + Arrays.toString(time_array_field) +
                ", timestamp_array_field=" + Arrays.toString(timestamp_array_field) +
                ", timestamptz_array_field=" + Arrays.toString(timestamptz_array_field) +
                '}';
    }
}
