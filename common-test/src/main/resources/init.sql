create table if not exists table_with_unique_field
(
    field_1 integer not null,
    field_2 varchar not null,
    unique (field_1)
);

alter table table_with_unique_field replica identity using index table_with_unique_field_field_1_key;

create table if not exists table_with_unique_field_combination
(
    field_1 integer not null,
    field_2 varchar not null,
    unique (field_1, field_2)
);

alter table table_with_unique_field_combination replica identity using index table_with_unique_field_combination_field_1_field_2_key;

create table if not exists table_with_full_replica_identity
(
    field_1 integer not null,
    field_2 varchar not null
);

alter table table_with_full_replica_identity replica identity full;


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
