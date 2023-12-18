package com.github.alexgaard.mirror.common_test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class DataTypesDbo {

    public Integer id;
    public Short int2_field;
    public Integer int4_field;
    public Long int8_field;
    public Float float4_field;
    public Double float8_field;
    public UUID uuid_field;
    public String varchar_field;
    public String text_field;
    public Boolean bool_field;
    public byte[] bytes_field;
    public Character char_field;
    public String json_field;
    public String jsonb_field;
    public LocalDate date_field;

    public LocalTime time_field;

    public LocalDateTime timestamp_field;
    public OffsetDateTime timestamptz_field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataTypesDbo dbo = (DataTypesDbo) o;
        return Objects.equals(id, dbo.id) && Objects.equals(int2_field, dbo.int2_field) && Objects.equals(int4_field, dbo.int4_field) && Objects.equals(int8_field, dbo.int8_field) && Objects.equals(float4_field, dbo.float4_field) && Objects.equals(float8_field, dbo.float8_field) && Objects.equals(uuid_field, dbo.uuid_field) && Objects.equals(varchar_field, dbo.varchar_field) && Objects.equals(text_field, dbo.text_field) && Objects.equals(bool_field, dbo.bool_field) && Arrays.equals(bytes_field, dbo.bytes_field) && Objects.equals(char_field, dbo.char_field) && Objects.equals(json_field, dbo.json_field) && Objects.equals(jsonb_field, dbo.jsonb_field) && Objects.equals(date_field, dbo.date_field) && Objects.equals(time_field, dbo.time_field) && Objects.equals(timestamp_field, dbo.timestamp_field) && Objects.equals(timestamptz_field, dbo.timestamptz_field);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, int2_field, int4_field, int8_field, float4_field, float8_field, uuid_field, varchar_field, text_field, bool_field, char_field, json_field, jsonb_field, date_field, time_field, timestamp_field, timestamptz_field);
        result = 31 * result + Arrays.hashCode(bytes_field);
        return result;
    }

    @Override
    public String toString() {
        return "DataTypesDbo{" +
                "id=" + id +
                ", int2_field=" + int2_field +
                ", int4_field=" + int4_field +
                ", int8_field=" + int8_field +
                ", float4_field=" + float4_field +
                ", float8_field=" + float8_field +
                ", uuid_field=" + uuid_field +
                ", varchar_field='" + varchar_field + '\'' +
                ", text_field='" + text_field + '\'' +
                ", bool_field=" + bool_field +
                ", bytes_field=" + Arrays.toString(bytes_field) +
                ", char_field=" + char_field +
                ", json_field='" + json_field + '\'' +
                ", jsonb_field='" + jsonb_field + '\'' +
                ", date_field=" + date_field +
                ", time_field=" + time_field +
                ", timestamp_field=" + timestamp_field +
                ", timestamptz_field=" + timestamptz_field +
                '}';
    }
}
