package com.github.alexgaard.mirror.postgres.processor;

import com.github.alexgaard.mirror.postgres.event.DeleteEvent;
import com.github.alexgaard.mirror.core.event.EventTransaction;
import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.postgres.event.InsertEvent;
import com.github.alexgaard.mirror.test_utils.DataTypesDbo;
import com.github.alexgaard.mirror.test_utils.DataTypesRepository;
import com.github.alexgaard.mirror.test_utils.DbUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.github.alexgaard.mirror.test_utils.AsyncUtils.eventually;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class PostgresEventProcessorTest {

    @Container
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16.0-alpine3.18")
            .withCommand("postgres", "-c", "wal_level=logical");

    private static DataSource dataSource;

    private static DataTypesRepository dataTypesRepository;

    @BeforeAll
    public static void setup() {
        dataSource = DbUtils.createDataSource(postgres);
        dataTypesRepository = new DataTypesRepository(dataSource);
        DbUtils.initTables(dataSource);
    }

    @Test
    public void should_handle_insert_event() {
        PostgresEventProcessor processor = new PostgresEventProcessor(dataSource);

        int id = 42;

        List<Field> fields = new ArrayList<>();
        fields.add(new Field("id", Field.Type.INT32, id));
        fields.add(new Field("int2_field", Field.Type.INT16, (short) 5));
        fields.add(new Field("int4_field",Field.Type.INT32, 42));
        fields.add(new Field("int8_field",Field.Type.INT64, 153L));
        fields.add(new Field("float4_field",Field.Type.FLOAT, 1.5423f));
        fields.add(new Field("float8_field",Field.Type.DOUBLE, 33.3099));
        fields.add(new Field("uuid_field",Field.Type.UUID, UUID.randomUUID()));
        fields.add(new Field("varchar_field",Field.Type.STRING, "test"));
        fields.add(new Field("text_field",Field.Type.STRING, "test2"));
        fields.add(new Field("bool_field",Field.Type.BOOLEAN, true));
        fields.add(new Field("bytes_field",Field.Type.BYTES, new byte[]{5, 87, 3}));
        fields.add(new Field("char_field",Field.Type.CHAR, 's'));
        fields.add(new Field("json_field",Field.Type.JSON, "{\"json\": true}"));
        fields.add(new Field("jsonb_field",Field.Type.JSON, "{\"json\": true}"));
        fields.add(new Field("date_field",Field.Type.DATE, LocalDate.now()));
        fields.add(new Field("time_field",Field.Type.TIME, LocalTime.now()));
        fields.add(new Field("timestamp_field",Field.Type.TIMESTAMP, LocalDateTime.now()));
        fields.add(new Field("timestamptz_field",Field.Type.TIMESTAMP_TZ, OffsetDateTime.now(Clock.systemUTC())));

        InsertEvent insert = new InsertEvent(
                UUID.randomUUID(),
                "public",
                "data_types",
                0,
                fields,
                OffsetDateTime.now()
        );

        processor.process(EventTransaction.of("test", insert));

        DataTypesDbo dataTypes = dataTypesRepository.getDataTypes(id)
                .orElseThrow();

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

    @Test
    public void should_handle_delete_event() {
        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = 78;

        dataTypesRepository.insertDataTypes(dbo);

        eventually(() -> {
            assertNotNull(dataTypesRepository.getDataTypes(dbo.id));
        });

        PostgresEventProcessor processor = new PostgresEventProcessor(dataSource);

        List<Field> fields = new ArrayList<>();
        fields.add(new Field("id", Field.Type.INT32, dbo.id));

        DeleteEvent delete = new DeleteEvent(
                UUID.randomUUID(),
                "public",
                "data_types",
                0,
                fields,
                OffsetDateTime.now()
        );

        processor.process(EventTransaction.of("test", delete));

        eventually(() -> {
            assertTrue(dataTypesRepository.getDataTypes(dbo.id).isEmpty());
        });
    }

    @Test
    public void should_skip_old_events() {
        PostgresEventProcessor processor = new PostgresEventProcessor(dataSource);

        int id1 = 67;
        int id2 = 68;

        InsertEvent insert1 = new InsertEvent(
                UUID.randomUUID(),
                "public",
                "data_types",
                10,
                List.of(new Field("id", Field.Type.INT32, id1)),
                OffsetDateTime.now()
        );

        InsertEvent insert2 = new InsertEvent(
                UUID.randomUUID(),
                "public",
                "data_types",
                9,
                List.of(new Field("id", Field.Type.INT32, id2)),
                OffsetDateTime.now()
        );

        processor.process(EventTransaction.of("test", insert1));
        processor.process(EventTransaction.of("test", insert2));

        Optional<DataTypesDbo> dataTypes1 = dataTypesRepository.getDataTypes(id1);
        Optional<DataTypesDbo> dataTypes2 = dataTypesRepository.getDataTypes(id2);

        assertTrue(dataTypes1.isPresent());
        assertTrue(dataTypes2.isEmpty());
    }

}
