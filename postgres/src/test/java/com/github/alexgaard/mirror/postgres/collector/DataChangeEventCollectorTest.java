package com.github.alexgaard.mirror.postgres.collector;

import com.github.alexgaard.mirror.common_test.*;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.Event;
import com.github.alexgaard.mirror.postgres.event.DeleteEvent;
import com.github.alexgaard.mirror.postgres.event.InsertEvent;
import com.github.alexgaard.mirror.postgres.event.PostgresTransactionEvent;
import com.github.alexgaard.mirror.postgres.event.UpdateEvent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.github.alexgaard.mirror.common_test.AsyncUtils.eventually;
import static com.github.alexgaard.mirror.common_test.DbUtils.drainWalMessages;
import static com.github.alexgaard.mirror.postgres.utils.CustomMessage.insertSkipTransactionMessage;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.jupiter.api.Assertions.*;

public class DataChangeEventCollectorTest {

    private static final DataSource dataSource = PostgresSingletonContainer.getDataSource();

    private static PostgresEventCollector collector;

    private static DataTypesRepository dataTypesRepository;

    private static final List<PostgresTransactionEvent> collectedTransactions = new CopyOnWriteArrayList<>();

    private static final String replicationName = "mirror_" + ((int) (Math.random() * 10_000));

    @BeforeAll
    public static void setup() {
        dataTypesRepository = new DataTypesRepository(dataSource);

        DbUtils.initTables(dataSource);

        PgReplication pgReplication = new PgReplication()
                .replicationSlotName(replicationName)
                .publicationName(replicationName)
                .allTables();

        pgReplication.setup(dataSource);

        collector = new PostgresEventCollector("test", dataSource, Duration.ofMillis(100), pgReplication);

        collector.setEventSink(transaction -> {
            if (transaction instanceof PostgresTransactionEvent) {
                collectedTransactions.add((PostgresTransactionEvent) transaction);
            }

            return Result.ok();
        });
    }

    @BeforeEach
    public void setupBefore() {
        collector.stop();
        collectedTransactions.clear();
    }

    @Test
    public void should_parse_insert_of_different_data_types() {
        drainWalMessages(dataSource, replicationName, replicationName);

        collector.start();

        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = (int) (Math.random() * 10000);
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
    public void should_parse_insert_of_null_data_types() {
        drainWalMessages(dataSource, replicationName, replicationName);

        collector.start();

        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = (int) (Math.random() * 10000);

        dataTypesRepository.insertDataTypes(dbo);

        eventually(() -> {
            assertEquals(1, collectedTransactions.size());

            InsertEvent dataChange = (InsertEvent) collectedTransactions.get(0).events.get(0);

            assertEquals("public", dataChange.namespace);
            assertEquals("data_types", dataChange.table);

            assertEquals(18, dataChange.fields.size());

            dataChange.fields.stream()
                    .filter(f -> !"id".equals(f.name))
                    .forEach(f -> assertNull(f.value));
        });
    }

    @Test
    public void should_handle_delete_event() {
        int id = 8797;

        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = id;
        dbo.int2_field = 5;

        dataTypesRepository.insertDataTypes(dbo);

        drainWalMessages(dataSource, replicationName, replicationName);

        collector.start();

        dataTypesRepository.deleteDataTypeRow(id);

        eventually(() -> {
            assertEquals(1, collectedTransactions.size());

            DeleteEvent deleteEvent = (DeleteEvent) collectedTransactions.get(0).events.get(0);

            assertNotNull(deleteEvent.id);
            assertEquals("data_types", deleteEvent.table);
            assertEquals("public", deleteEvent.namespace);
            assertEquals(18, deleteEvent.identifierFields.size());
            assertTrue(deleteEvent.identifierFields.stream().anyMatch(f -> f.name.equals("id") && ((Integer) id).equals(f.value)));
        });
    }

    @Test
    public void should_skip_transaction_with_skip_message() {
        collector.start();

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("insert into data_types (id) values (89)");
            }

            insertSkipTransactionMessage(connection);
            connection.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("insert into data_types (id) values (90)");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        eventually(() -> {
            assertEquals(1, collectedTransactions.size());
        });
    }

    @Test
    public void should_handle_update_event() {
        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = 87;
        dbo.int2_field = 5;

        dataTypesRepository.insertDataTypes(dbo);

        drainWalMessages(dataSource, replicationName, replicationName);

        collector.start();

        DataTypesDbo update = new DataTypesDbo();
        update.id = 87;
        update.int2_field = 5;
        update.int4_field = 100;
        update.int8_field = 48L;
        update.float4_field = 5.32f;
        update.float8_field = 8932.43;
        update.uuid_field = UUID.randomUUID();
        update.varchar_field = "varchar";
        update.text_field = "text";
        update.bool_field = true;
        update.bytes_field = new byte[]{5, 6, 9};
        update.char_field = 'C';
        update.json_field = "{\"json\": true}";
        update.jsonb_field = "{\"json\": true}";
        update.date_field = LocalDate.now();
        update.time_field = LocalTime.now();
        update.timestamp_field = LocalDateTime.now();
        update.timestamptz_field = OffsetDateTime.now();

        dataTypesRepository.updateDataTypes(update);

        eventually(() -> {
            assertEquals(1, collectedTransactions.size());

            UpdateEvent updateEvent = (UpdateEvent) collectedTransactions.get(0).events.get(0);

            assertNotNull(updateEvent.id);

            assertEquals("int2_field", updateEvent.fields.get(0).name);
            assertEquals(update.int2_field, updateEvent.fields.get(0).value);

            assertEquals("int4_field", updateEvent.fields.get(1).name);
            assertEquals(update.int4_field, updateEvent.fields.get(1).value);

            assertEquals("int8_field", updateEvent.fields.get(2).name);
            assertEquals(update.int8_field, updateEvent.fields.get(2).value);

            assertEquals("float4_field", updateEvent.fields.get(3).name);
            assertEquals(update.float4_field, updateEvent.fields.get(3).value);

            assertEquals("float8_field", updateEvent.fields.get(4).name);
            assertEquals(update.float8_field, updateEvent.fields.get(4).value);

            assertEquals("uuid_field", updateEvent.fields.get(5).name);
            assertEquals(update.uuid_field, updateEvent.fields.get(5).value);

            assertEquals("varchar_field", updateEvent.fields.get(6).name);
            assertEquals(update.varchar_field, updateEvent.fields.get(6).value);

            assertEquals("text_field", updateEvent.fields.get(7).name);
            assertEquals(update.text_field, updateEvent.fields.get(7).value);

            assertEquals("bool_field", updateEvent.fields.get(8).name);
            assertEquals(update.bool_field, updateEvent.fields.get(8).value);

            assertEquals("bytes_field", updateEvent.fields.get(9).name);
            assertArrayEquals(update.bytes_field, (byte[]) updateEvent.fields.get(9).value);

            assertEquals("char_field", updateEvent.fields.get(10).name);
            assertEquals(update.char_field, updateEvent.fields.get(10).value);

            assertEquals("json_field", updateEvent.fields.get(11).name);
            assertEquals(update.json_field, updateEvent.fields.get(11).value);

            assertEquals("jsonb_field", updateEvent.fields.get(12).name);
            assertEquals(update.jsonb_field, updateEvent.fields.get(12).value);

            assertEquals("date_field", updateEvent.fields.get(13).name);
            assertEquals(update.date_field, updateEvent.fields.get(13).value);

            assertEquals("time_field", updateEvent.fields.get(14).name);
            assertEquals(update.time_field.truncatedTo(MILLIS), ((LocalTime) updateEvent.fields.get(14).value).truncatedTo(MILLIS));

            assertEquals("timestamp_field", updateEvent.fields.get(15).name);
            assertEquals(update.timestamp_field.truncatedTo(MILLIS), ((LocalDateTime) updateEvent.fields.get(15).value).truncatedTo(MILLIS));

            assertEquals("timestamptz_field", updateEvent.fields.get(16).name);
            assertEquals(update.timestamptz_field.truncatedTo(MILLIS), ((OffsetDateTime) updateEvent.fields.get(16).value).truncatedTo(MILLIS));
        });
    }

    @Test
    public void should_handle_update_event_with_unique_index() {
        QueryUtils.update(dataSource, "insert into table_with_unique_field(field_1, field_2) values(1, 'hello')");

        drainWalMessages(dataSource, replicationName, replicationName);

        collector.start();

        QueryUtils.update(dataSource, "update table_with_unique_field set field_2 = 'world' where field_1 = 1");

        eventually(() -> {
            assertEquals(1, collectedTransactions.size());
            PostgresTransactionEvent transaction = collectedTransactions.get(0);

            assertEquals(1, transaction.events.size());

            UpdateEvent event = (UpdateEvent) transaction.events.get(0);

            assertEquals(1, event.identifierFields.size());
            assertEquals("field_1", event.identifierFields.get(0).name);
            assertEquals(1, event.identifierFields.get(0).value);

            assertEquals(1, event.fields.size());
            assertEquals("field_2", event.fields.get(0).name);
            assertEquals("world", event.fields.get(0).value);
        });
    }

    @Test
    public void should_handle_update_event_with_combined_unique_index() {
        QueryUtils.update(dataSource, "insert into table_with_unique_field_combination(field_1, field_2) values(1, 'hello')");

        drainWalMessages(dataSource, replicationName, replicationName);

        collector.start();

        QueryUtils.update(dataSource, "update table_with_unique_field_combination set field_2 = 'world' where field_1 = 1");

        eventually(() -> {
            assertEquals(1, collectedTransactions.size());
            PostgresTransactionEvent transaction = collectedTransactions.get(0);

            assertEquals(1, transaction.events.size());

            UpdateEvent event = (UpdateEvent) transaction.events.get(0);

            assertEquals(2, event.identifierFields.size());
            assertEquals("field_1", event.identifierFields.get(0).name);
            assertEquals(1, event.identifierFields.get(0).value);
            assertEquals("field_2", event.identifierFields.get(1).name);
            assertEquals("hello", event.identifierFields.get(1).value);

            assertEquals(1, event.fields.size());
            assertEquals("field_2", event.fields.get(0).name);
            assertEquals("world", event.fields.get(0).value);
        });
    }

    @Test
    public void should_handle_update_event_with_full_replica_identity() {
        QueryUtils.update(dataSource, "insert into table_with_full_replica_identity(field_1, field_2) values(1, 'hello')");

        drainWalMessages(dataSource, replicationName, replicationName);

        collector.start();

        QueryUtils.update(dataSource, "update table_with_full_replica_identity set field_2 = 'world' where field_1 = 1 and field_2 = 'hello'");

        eventually(() -> {
            assertEquals(1, collectedTransactions.size());
            PostgresTransactionEvent transaction = collectedTransactions.get(0);

            assertEquals(1, transaction.events.size());

            UpdateEvent event = (UpdateEvent) transaction.events.get(0);

            assertEquals(2, event.identifierFields.size());
            assertEquals("field_1", event.identifierFields.get(0).name);
            assertEquals(1, event.identifierFields.get(0).value);
            assertEquals("field_2", event.identifierFields.get(1).name);
            assertEquals("hello", event.identifierFields.get(1).value);

            assertEquals(2, event.fields.size());
            assertEquals("field_1", event.fields.get(0).name);
            assertEquals(1, event.fields.get(0).value);
            assertEquals("field_2", event.fields.get(1).name);
            assertEquals("world", event.fields.get(1).value);
        });
    }

}
