package com.github.alexgaard.mirror.common_test;

public class InitSql {

    public static final String createTableDataTypesSql = "create table if not exists data_types(\n" +
            "    id serial primary key,\n" +
            "    int2_field int2,\n" +
            "    int4_field int4,\n" +
            "    int8_field int8,\n" +
            "    float4_field float4,\n" +
            "    float8_field float8,\n" +
            "    uuid_field uuid,\n" +
            "    varchar_field varchar,\n" +
            "    text_field text,\n" +
            "    bool_field bool,\n" +
            "    bytes_field bytea,\n" +
            "    char_field char,\n" +
            "    json_field json,\n" +
            "    jsonb_field jsonb,\n" +
            "    date_field date,\n" +
            "    time_field time,\n" +
            "    timestamp_field timestamp,\n" +
            "    timestamptz_field timestamp with time zone\n" +
            ");";

}
