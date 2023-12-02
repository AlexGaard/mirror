package com.github.alexgaard.mirror.postgres.collector;

import com.github.alexgaard.mirror.core.event.DeleteEvent;
import com.github.alexgaard.mirror.core.event.EventTransaction;
import com.github.alexgaard.mirror.core.event.InsertEvent;
import com.github.alexgaard.mirror.postgres.utils.QueryUtils;
import com.github.alexgaard.mirror.test_utils.DataTypesDbo;
import com.github.alexgaard.mirror.test_utils.DataTypesRepository;
import com.github.alexgaard.mirror.test_utils.DbUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Types;
import java.time.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.github.alexgaard.mirror.test_utils.AsyncUtils.eventually;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class PostgresEventCollectorTest {

    @Container
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16.0-alpine3.18")
            .withCommand("postgres", "-c", "wal_level=logical");

    private static DataSource dataSource;

    private static PostgresEventCollector collector;

    private static DataTypesRepository dataTypesRepository;

    private static final List<EventTransaction> collectedTransactions = new CopyOnWriteArrayList<>();

    @BeforeAll
    public static void setup() {
        dataSource = DbUtils.createDataSource(postgres);
        dataTypesRepository = new DataTypesRepository(dataSource);

        DbUtils.initTables(dataSource);

        PgReplication pgReplication = new PgReplication()
                .allTables();

        collector = new PostgresEventCollector("test", dataSource, Duration.ofMillis(100), pgReplication);

        collector.setOnTranscationCollected((collectedTransactions::add));
    }

    @BeforeEach
    public void setupBefore() {
        collector.stop();
        collectedTransactions.clear();
    }

    @Test
    public void should_parse_insert_of_different_data_types() {
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

        dataTypesRepository.insertDataTypes(dbo);

        eventually(() -> {
            assertEquals(1, collectedTransactions.size());

            InsertEvent dataChange = (InsertEvent) collectedTransactions.get(0).events.get(0);

            assertEquals("public", dataChange.namespace);
            assertEquals("data_types", dataChange.table);
            assertEquals(18, dataChange.fields.size());

            assertEquals("id", dataChange.fields.get(0).name);
            assertNotNull(dataChange.fields.get(0).value);

            assertEquals("int2_field", dataChange.fields.get(1).name);
            assertEquals(dbo.int2_field, dataChange.fields.get(1).value);

            assertEquals("int4_field", dataChange.fields.get(2).name);
            assertEquals(dbo.int4_field, dataChange.fields.get(2).value);

            assertEquals("int8_field", dataChange.fields.get(3).name);
            assertEquals(dbo.int8_field, dataChange.fields.get(3).value);

            assertEquals("float4_field", dataChange.fields.get(4).name);
            assertEquals(dbo.float4_field, dataChange.fields.get(4).value);

            assertEquals("float8_field", dataChange.fields.get(5).name);
            assertEquals(dbo.float8_field, dataChange.fields.get(5).value);

            assertEquals("uuid_field", dataChange.fields.get(6).name);
            assertEquals(dbo.uuid_field, dataChange.fields.get(6).value);

            assertEquals("varchar_field", dataChange.fields.get(7).name);
            assertEquals(dbo.varchar_field, dataChange.fields.get(7).value);

            assertEquals("text_field", dataChange.fields.get(8).name);
            assertEquals(dbo.text_field, dataChange.fields.get(8).value);

            assertEquals("bool_field", dataChange.fields.get(9).name);
            assertEquals(dbo.bool_field, dataChange.fields.get(9).value);

            assertEquals("bytes_field", dataChange.fields.get(10).name);
            assertArrayEquals(dbo.bytes_field, (byte[]) dataChange.fields.get(10).value);

            assertEquals("char_field", dataChange.fields.get(11).name);
            assertEquals(dbo.char_field, dataChange.fields.get(11).value);

            assertEquals("json_field", dataChange.fields.get(12).name);
            assertEquals(dbo.json_field, dataChange.fields.get(12).value);

            assertEquals("jsonb_field", dataChange.fields.get(13).name);
            assertEquals(dbo.jsonb_field, dataChange.fields.get(13).value);

            assertEquals("date_field", dataChange.fields.get(14).name);
            assertEquals(dbo.date_field, dataChange.fields.get(14).value);

            assertEquals("time_field", dataChange.fields.get(15).name);
            assertEquals(dbo.time_field.truncatedTo(MILLIS), ((LocalTime) dataChange.fields.get(15).value).truncatedTo(MILLIS));

            assertEquals("timestamp_field", dataChange.fields.get(16).name);
            assertEquals(dbo.timestamp_field.truncatedTo(MILLIS), ((LocalDateTime) dataChange.fields.get(16).value).truncatedTo(MILLIS));

            assertEquals("timestamptz_field", dataChange.fields.get(17).name);
            assertEquals(dbo.timestamptz_field.truncatedTo(MILLIS), ((OffsetDateTime) dataChange.fields.get(17).value).truncatedTo(MILLIS));
        });
    }

    @Test
    public void should_handle_delete_event() {
        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = 42;
        dbo.int2_field = 5;

        dataTypesRepository.insertDataTypes(dbo);

        collector.start();

        dataTypesRepository.deleteDataTypeRow(42);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        eventually(() -> {
            assertEquals(1, collectedTransactions.size());

            DeleteEvent deleteEvent = (DeleteEvent) collectedTransactions.get(0).events.get(0);

            assertNotNull(deleteEvent.id);
            assertEquals("data_types", deleteEvent.table);
            assertEquals("public", deleteEvent.namespace);
            assertEquals(18, deleteEvent.identifyingFields.size());
            assertTrue(deleteEvent.identifyingFields.stream().anyMatch(f -> f.name.equals("id") && ((Integer) 42).equals(f.value)));
        });
    }


}
