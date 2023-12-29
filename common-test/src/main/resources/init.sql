create table if not exists table_with_unique_field
(
    field_1 integer not null,
    field_2 varchar not null,
    unique (field_1)
);

create table if not exists table_with_unique_field_combination
(
    field_1 integer not null,
    field_2 boolean,
    field_3 varchar not null,
    unique (field_1, field_3)
);

create table if not exists table_with_nullable_unique_field_combination
(
    field_1 integer not null,
    field_2 boolean,
    field_3 varchar,
    unique (field_1, field_3)
);

create table if not exists table_with_multiple_unique
(
    field_1 integer not null,
    field_2 varchar not null,
    unique (field_1),
    unique (field_2)
);

create table if not exists table_without_key
(
    field_1 integer not null,
    field_2 varchar,
    field_3 boolean
);

create table if not exists data_types
(
    id                serial primary key,
    int2_field        int2,
    int4_field        int4,
    int8_field        int8,
    float4_field      float4,
    float8_field      float8,
    uuid_field        uuid,
    varchar_field     varchar,
    text_field        text,
    bool_field        bool,
    bytes_field       bytea,
    char_field        char,
    json_field        json,
    jsonb_field       jsonb,
    date_field        date,
    time_field        time,
    timestamp_field   timestamp,
    timestamptz_field timestamp with time zone
);
