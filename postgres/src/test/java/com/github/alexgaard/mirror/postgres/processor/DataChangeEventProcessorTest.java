package com.github.alexgaard.mirror.postgres.processor;

import com.github.alexgaard.mirror.common_test.DataTypesDbo;
import com.github.alexgaard.mirror.common_test.DataTypesRepository;
import com.github.alexgaard.mirror.common_test.DbUtils;
import com.github.alexgaard.mirror.common_test.PostgresSingletonContainer;
import com.github.alexgaard.mirror.core.Event;
import com.github.alexgaard.mirror.postgres.event.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.github.alexgaard.mirror.common_test.AsyncUtils.eventually;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.jupiter.api.Assertions.*;

public class DataChangeEventProcessorTest {

    private static final DataSource dataSource = PostgresSingletonContainer.getDataSource();

    private static DataTypesRepository dataTypesRepository;

    @BeforeAll
    public static void setup() {
        dataTypesRepository = new DataTypesRepository(dataSource);
        DbUtils.initTables(dataSource);
    }

    @Test
    public void should_handle_insert_event() {
        PostgresEventProcessor processor = new PostgresEventProcessor(dataSource);

        int id = 42;

        List<Field<?>> fields = new ArrayList<>();
        fields.add(new Field.Int32("id", id));
        fields.add(new Field.Int16("int2_field", (short) 5));
        fields.add(new Field.Int32("int4_field", 42));
        fields.add(new Field.Int64("int8_field", 153L));
        fields.add(new Field.Float("float4_field", 1.5423f));
        fields.add(new Field.Double("float8_field", 33.3099));
        fields.add(new Field.Uuid("uuid_field", UUID.randomUUID()));
        fields.add(new Field.Text("varchar_field", "test"));
        fields.add(new Field.Text("text_field", "test2"));
        fields.add(new Field.Boolean("bool_field", true));
        fields.add(new Field.Bytes("bytes_field", new byte[]{5, 87, 3}));
        fields.add(new Field.Char("char_field", 's'));
        fields.add(new Field.Json("json_field", "{\"json\": true}"));
        fields.add(new Field.Json("jsonb_field", "{\"json\": true}"));
        fields.add(new Field.Date("date_field", LocalDate.now()));
        fields.add(new Field.Time("time_field", LocalTime.now()));
        fields.add(new Field.Timestamp("timestamp_field", LocalDateTime.now()));
        fields.add(new Field.TimestampTz("timestamptz_field", OffsetDateTime.now(Clock.systemUTC())));

        InsertEvent insert = new InsertEvent(
                UUID.randomUUID(),
                "public",
                "data_types",
                1,
                fields
        );

        processor.consume(PostgresTransactionEvent.of("test", insert));

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
    public void should_handle_update_event() {
        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = 859;

        dataTypesRepository.insertDataTypes(dbo);

        eventually(() -> {
            assertNotNull(dataTypesRepository.getDataTypes(dbo.id));
        });

        PostgresEventProcessor processor = new PostgresEventProcessor(dataSource);

        List<Field<?>> idFields = List.of(
                new Field.Int32("id", dbo.id)
        );

        List<Field<?>> updateFields = List.of(
                new Field.Text("text_field", "hello")
        );

        UpdateEvent update = new UpdateEvent(
                UUID.randomUUID(),
                "public",
                "data_types",
                6,
                idFields,
                updateFields
        );

        processor.consume(PostgresTransactionEvent.of("test", update));

        eventually(() -> {
            assertEquals("hello", dataTypesRepository.getDataTypes(dbo.id).get().text_field);
        });
    }

    @Test
    public void should_handle_update_event_with_multiple_id_fields() {
        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = 546;
        dbo.bool_field = true;

        dataTypesRepository.insertDataTypes(dbo);

        eventually(() -> {
            assertNotNull(dataTypesRepository.getDataTypes(dbo.id));
        });

        PostgresEventProcessor processor = new PostgresEventProcessor(dataSource);

        List<Field<?>> idFields = List.of(
                new Field.Int32("id", dbo.id),
                new Field.Boolean("bool_field", true)
        );

        List<Field<?>> updateFields = List.of(
                new Field.Text("text_field", "hello")
        );

        UpdateEvent update = new UpdateEvent(
                UUID.randomUUID(),
                "public",
                "data_types",
                6,
                idFields,
                updateFields
        );

        processor.consume(PostgresTransactionEvent.of("test", update));

        eventually(() -> {
            assertEquals("hello", dataTypesRepository.getDataTypes(dbo.id).get().text_field);
        });
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

        List<Field<?>> fields = new ArrayList<>();
        fields.add(new Field<>("id", Field.Type.INT32, dbo.id));

        DeleteEvent delete = new DeleteEvent(
                UUID.randomUUID(),
                "public",
                "data_types",
                1,
                fields
        );

        processor.consume(PostgresTransactionEvent.of("test", delete));

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
                List.of(new Field<>("id", Field.Type.INT32, id1))
        );

        InsertEvent insert2 = new InsertEvent(
                UUID.randomUUID(),
                "public",
                "data_types",
                9,
                List.of(new Field<>("id", Field.Type.INT32, id2))
        );

        processor.consume(PostgresTransactionEvent.of("test", insert1));
        processor.consume(PostgresTransactionEvent.of("test", insert2));

        Optional<DataTypesDbo> dataTypes1 = dataTypesRepository.getDataTypes(id1);
        Optional<DataTypesDbo> dataTypes2 = dataTypesRepository.getDataTypes(id2);

        assertTrue(dataTypes1.isPresent());
        assertTrue(dataTypes2.isEmpty());
    }

}
