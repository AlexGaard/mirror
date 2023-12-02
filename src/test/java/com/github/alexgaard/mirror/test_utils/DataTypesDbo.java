package com.github.alexgaard.mirror.test_utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
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
    
}
