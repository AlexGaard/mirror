
create table if not exists data_types(
    id serial primary key,
    int2_field int2,
    int4_field int4,
    int8_field int8,
    float4_field float4,
    float8_field float8,
    uuid_field uuid,
    varchar_field varchar,
    text_field text,
    bool_field bool,
    bytes_field bytea,
    char_field char,
    json_field json,
    jsonb_field jsonb,
    date_field date,
    time_field time,
    timestamp_field timestamp,
    timestamptz_field timestamp with time zone
);
