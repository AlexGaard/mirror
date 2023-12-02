package com.github.alexgaard.mirror.postgres.processor;

import com.github.alexgaard.mirror.core.event.EventTransaction;
import com.github.alexgaard.mirror.core.event.Field;
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
import java.sql.*;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.github.alexgaard.mirror.postgres.utils.QueryUtils.resultList;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class PostgresEventProcessorTest {

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
    public void should_handle_insert_event() {
        PostgresEventProcessor processor = new PostgresEventProcessor(dataSource);

        int id = 42;

        List<Field> fields = new ArrayList<>();
        fields.add(new Field("id", id, Field.Type.INT32));
        fields.add(new Field("int2_field", (short) 5, Field.Type.INT16));
        fields.add(new Field("int4_field", 42, Field.Type.INT32));
        fields.add(new Field("int8_field", 153L, Field.Type.INT64));
        fields.add(new Field("float4_field", 1.5423f, Field.Type.FLOAT));
        fields.add(new Field("float8_field", 33.3099, Field.Type.DOUBLE));
        fields.add(new Field("uuid_field", UUID.randomUUID(), Field.Type.UUID));
        fields.add(new Field("varchar_field", "test", Field.Type.STRING));
        fields.add(new Field("text_field", "test2", Field.Type.STRING));
        fields.add(new Field("bool_field", true, Field.Type.BOOLEAN));
        fields.add(new Field("bytes_field", new byte[]{5, 87, 3}, Field.Type.BYTES));
        fields.add(new Field("char_field", 's', Field.Type.CHAR));
        fields.add(new Field("json_field", "{\"json\": true}", Field.Type.JSON));
        fields.add(new Field("jsonb_field", "{\"json\": true}", Field.Type.JSON));
        fields.add(new Field("date_field", LocalDate.now(), Field.Type.DATE));
        fields.add(new Field("time_field", LocalTime.now(), Field.Type.TIME));
        fields.add(new Field("timestamp_field", LocalDateTime.now(), Field.Type.TIMESTAMP));
        fields.add(new Field("timestamptz_field", OffsetDateTime.now(Clock.systemUTC()), Field.Type.TIMESTAMP_TZ));

        InsertEvent insert = new InsertEvent(
                UUID.randomUUID(),
                System.currentTimeMillis(),
                "public",
                "data_types",
                fields
        );

        processor.process(EventTransaction.of("test", insert));

        DataTypesDbo dataTypes = getDataTypes(id);

        assertEquals(fields.get(0).value, dataTypes.id);
        assertEquals(fields.get(1).value, dataTypes.int2_field);
        assertEquals(fields.get(2).value, dataTypes.int4_field);
        assertEquals(fields.get(3).value, dataTypes.int8_field);
        assertEquals(fields.get(4).value, dataTypes.float4_field);
        assertEquals(fields.get(5).value, dataTypes.float8_field);
        assertEquals(fields.get(6).value, dataTypes.uuid_field);
        assertEquals(fields.get(7).value, dataTypes.varchar_field);
        assertEquals(fields.get(8).value, dataTypes.text_field);
        assertEquals(fields.get(9).value, dataTypes.bool_field);
        assertArrayEquals((byte[]) fields.get(10).value, dataTypes.bytes_field);
        assertEquals(fields.get(11).value, dataTypes.char_field);
        assertEquals(fields.get(12).value, dataTypes.json_field);
        assertEquals(fields.get(13).value, dataTypes.jsonb_field);
        assertEquals(fields.get(14).value, dataTypes.date_field);
        assertEquals(((LocalTime) fields.get(15).value).truncatedTo(MILLIS), dataTypes.time_field.truncatedTo(MILLIS));
        assertEquals(((LocalDateTime) fields.get(16).value).truncatedTo(MILLIS), dataTypes.timestamp_field.truncatedTo(MILLIS));
        assertEquals(((OffsetDateTime) fields.get(17).value).truncatedTo(MILLIS), dataTypes.timestamptz_field.truncatedTo(MILLIS));
    }

    private DataTypesDbo getDataTypes(int id) {
        return QueryUtils.query(dataSource, "SELECT * FROM data_types WHERE id = ?", statement -> {
            statement.setInt(1, id);
            ResultSet resultSet = statement.executeQuery();

            return resultList(resultSet, PostgresEventProcessorTest::mapRowToDataTypesDbo)
                    .stream()
                    .findAny()
                    .orElseThrow();
        });
    }

    private static DataTypesDbo mapRowToDataTypesDbo(ResultSet rs) throws SQLException {
        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = rs.getInt("id");
        dbo.int2_field = rs.getShort("int2_field");
        dbo.int4_field = rs.getInt("int4_field");
        dbo.int8_field = rs.getLong("int8_field");
        dbo.float4_field = rs.getFloat("float4_field");
        dbo.float8_field = rs.getDouble("float8_field");
        dbo.uuid_field = uuid(rs.getString("uuid_field"));
        dbo.varchar_field = rs.getString("varchar_field");
        dbo.text_field = rs.getString("text_field");
        dbo.bool_field = rs.getBoolean("bool_field");
        dbo.bytes_field = rs.getBytes("bytes_field");
        dbo.char_field = character(rs.getString("char_field"));
        dbo.json_field = rs.getString("json_field");
        dbo.jsonb_field = rs.getString("jsonb_field");
        dbo.date_field = rs.getObject("date_field", LocalDate.class);
        dbo.time_field = rs.getObject("time_field", LocalTime.class);
        dbo.timestamp_field = rs.getObject("timestamp_field", LocalDateTime.class);
        dbo.timestamptz_field = rs.getObject("timestamptz_field", OffsetDateTime.class);
        return dbo;
    }

    private static UUID uuid(String str) {
        return str != null ? UUID.fromString(str) : null;
    }

    private static Character character(String str) {
        return str != null ? str.charAt(0) : null;
    }

}
