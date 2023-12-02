package com.github.alexgaard.mirror.postgres.collector;

import com.github.alexgaard.mirror.core.event.Event;
import com.github.alexgaard.mirror.core.event.InsertEvent;
import com.github.alexgaard.mirror.postgres.utils.QueryUtils;
import com.github.alexgaard.mirror.test_utils.DataTypesDbo;
import com.github.alexgaard.mirror.test_utils.DbUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Types;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.github.alexgaard.mirror.test_utils.AsyncUtils.eventually;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class PostgresEventCollectorTest {

    @Container
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16.0-alpine3.18")
            .withCommand("postgres", "-c", "wal_level=logical");

    private static DataSource dataSource;

    @BeforeAll
    public static void setup() {
        dataSource = DbUtils.createDataSource(postgres);
        DbUtils.initTables(dataSource);
    }

    @Test
    public void should_parse_insert_of_different_data_types() {
        PgReplication pgReplication = new PgReplication()
                .allTables();

        PostgresEventCollector collector = new PostgresEventCollector("test", dataSource, Duration.ofMillis(100), pgReplication);

        List<Event> changes = new ArrayList<>();

        collector.initialize((transaction) -> {
            changes.clear();
            changes.addAll(transaction.events);
        });

        collector.start();

        DataTypesDbo dbo = new DataTypesDbo();
        dbo.int2_field = 5;
        dbo.int4_field = 100;
        dbo.int8_field = 48L;
        dbo.float4_field = 5.32f;
        dbo.float8_field = 8932.43;
        dbo.uuid_field = UUID.randomUUID();
        dbo.varchar_field = "varchar";
        dbo.text_field = "text";
        dbo.bool_field = true;
        dbo.bytes_field = new byte[]{5, 6, 9};
        dbo.char_field = 'C';
        dbo.json_field = "{\"json\": true}";
        dbo.jsonb_field = "{\"json\": true}";
        dbo.date_field = LocalDate.now();
        dbo.time_field = LocalTime.now();
        dbo.timestamp_field = LocalDateTime.now();
        dbo.timestamptz_field = OffsetDateTime.now();

        insertDataTypes(dbo);

        eventually(Duration.ofSeconds(3), () -> {
            assertEquals(1, changes.size());

            InsertEvent dataChange = (InsertEvent) changes.get(0);

            assertEquals("public", dataChange.namespace);
            assertEquals("data_types", dataChange.table);
            assertEquals(17, dataChange.fields.size());

            assertEquals("int2_field", dataChange.fields.get(0).name);
            assertEquals(dbo.int2_field, dataChange.fields.get(0).value);

            assertEquals("int4_field", dataChange.fields.get(1).name);
            assertEquals(dbo.int4_field, dataChange.fields.get(1).value);

            assertEquals("int8_field", dataChange.fields.get(2).name);
            assertEquals(dbo.int8_field, dataChange.fields.get(2).value);

            assertEquals("float4_field", dataChange.fields.get(3).name);
            assertEquals(dbo.float4_field, dataChange.fields.get(3).value);

            assertEquals("float8_field", dataChange.fields.get(4).name);
            assertEquals(dbo.float8_field, dataChange.fields.get(4).value);

            assertEquals("uuid_field", dataChange.fields.get(5).name);
            assertEquals(dbo.uuid_field, dataChange.fields.get(5).value);

            assertEquals("varchar_field", dataChange.fields.get(6).name);
            assertEquals(dbo.varchar_field, dataChange.fields.get(6).value);

            assertEquals("text_field", dataChange.fields.get(7).name);
            assertEquals(dbo.text_field, dataChange.fields.get(7).value);

            assertEquals("bool_field", dataChange.fields.get(8).name);
            assertEquals(dbo.bool_field, dataChange.fields.get(8).value);

            assertEquals("bytes_field", dataChange.fields.get(9).name);
            assertArrayEquals(dbo.bytes_field, (byte[]) dataChange.fields.get(9).value);

            assertEquals("char_field", dataChange.fields.get(10).name);
            assertEquals(dbo.char_field, dataChange.fields.get(10).value);

            assertEquals("json_field", dataChange.fields.get(11).name);
            assertEquals(dbo.json_field, dataChange.fields.get(11).value);

            assertEquals("jsonb_field", dataChange.fields.get(12).name);
            assertEquals(dbo.jsonb_field, dataChange.fields.get(12).value);

            assertEquals("date_field", dataChange.fields.get(13).name);
            assertEquals(dbo.date_field, dataChange.fields.get(13).value);

            assertEquals("time_field", dataChange.fields.get(14).name);
            assertEquals(dbo.time_field.truncatedTo(MILLIS), ((LocalTime) dataChange.fields.get(14).value).truncatedTo(MILLIS));

            assertEquals("timestamp_field", dataChange.fields.get(15).name);
            assertEquals(dbo.timestamp_field.truncatedTo(MILLIS), ((LocalDateTime) dataChange.fields.get(15).value).truncatedTo(MILLIS));

            assertEquals("timestamptz_field", dataChange.fields.get(16).name);
            assertEquals(dbo.timestamptz_field.truncatedTo(MILLIS), ((OffsetDateTime) dataChange.fields.get(16).value).truncatedTo(MILLIS));
        });
    }

    private void insertDataTypes(DataTypesDbo dbo) {
        String sql = "insert into data_types (" +
                "int2_field," +
                "int4_field," +
                "int8_field," +
                "float4_field," +
                "float8_field," +
                "uuid_field," +
                "varchar_field," +
                "text_field," +
                "bool_field," +
                "bytes_field," +
                "char_field," +
                "json_field," +
                "jsonb_field," +
                "date_field," +
                "time_field," +
                "timestamp_field," +
                "timestamptz_field" +
                ") values (?,?,?,?,?,?::uuid,?,?,?,?,?,?::json,?::jsonb,?,?,?,?)";

        QueryUtils.update(dataSource, sql, statement -> {
            if (dbo.int2_field != null) {
                statement.setShort(1, dbo.int2_field);
            } else {
                statement.setNull(1, Types.SMALLINT);
            }

            if (dbo.int4_field != null) {
                statement.setInt(2, dbo.int4_field);
            } else {
                statement.setNull(2, Types.INTEGER);
            }

            if (dbo.int8_field != null) {
                statement.setLong(3, dbo.int8_field);
            } else {
                statement.setNull(3, Types.BIGINT);
            }

            if (dbo.float4_field != null) {
                statement.setFloat(4, dbo.float4_field);
            } else {
                statement.setNull(4, Types.FLOAT);
            }

            if (dbo.float8_field != null) {
                statement.setDouble(5, dbo.float8_field);
            } else {
                statement.setNull(5, Types.DOUBLE);
            }

            if (dbo.uuid_field != null) {
                statement.setString(6, dbo.uuid_field.toString());
            } else {
                statement.setNull(6, Types.VARCHAR);
            }

            if (dbo.varchar_field != null) {
                statement.setString(7, dbo.varchar_field);
            } else {
                statement.setNull(7, Types.VARCHAR);
            }

            if (dbo.text_field != null) {
                statement.setString(8, dbo.text_field);
            } else {
                statement.setNull(8, Types.VARCHAR);
            }

            if (dbo.bool_field != null) {
                statement.setBoolean(9, dbo.bool_field);
            } else {
                statement.setNull(9, Types.BOOLEAN);
            }

            if (dbo.bytes_field != null) {
                statement.setBytes(10, dbo.bytes_field);
            } else {
                statement.setNull(10, Types.BINARY);
            }

            if (dbo.char_field != null) {
                statement.setString(11, dbo.char_field.toString());
            } else {
                statement.setNull(11, Types.CHAR);
            }

            if (dbo.json_field != null) {
                statement.setString(12, dbo.json_field);
            } else {
                statement.setNull(12, Types.VARCHAR);
            }

            if (dbo.jsonb_field != null) {
                statement.setString(13, dbo.jsonb_field);
            } else {
                statement.setNull(13, Types.VARCHAR);
            }

            if (dbo.date_field != null) {
                statement.setObject(14, dbo.date_field);
            } else {
                statement.setNull(14, Types.DATE);
            }

            if (dbo.time_field != null) {
                statement.setObject(15, dbo.time_field);
            } else {
                statement.setNull(15, Types.TIME);
            }

            if (dbo.timestamp_field != null) {
                statement.setObject(16, dbo.timestamp_field);
            } else {
                statement.setNull(16, Types.TIMESTAMP);
            }

            if (dbo.timestamptz_field != null) {
                statement.setObject(17, dbo.timestamptz_field);
            } else {
                statement.setNull(17, Types.TIME_WITH_TIMEZONE);
            }

            statement.executeUpdate();
        });
    }

}
