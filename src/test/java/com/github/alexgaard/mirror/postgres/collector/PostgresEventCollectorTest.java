package com.github.alexgaard.mirror.postgres.collector;

import com.github.alexgaard.mirror.core.event.DeleteEvent;
import com.github.alexgaard.mirror.core.event.EventTransaction;
import com.github.alexgaard.mirror.core.event.InsertEvent;
import com.github.alexgaard.mirror.core.event.UpdateEvent;
import com.github.alexgaard.mirror.postgres.utils.QueryUtils;
import com.github.alexgaard.mirror.test_utils.DataTypesDbo;
import com.github.alexgaard.mirror.test_utils.DataTypesRepository;
import com.github.alexgaard.mirror.test_utils.DbUtils;
import com.github.alexgaard.mirror.test_utils.PostgresSingletonContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Types;
import java.time.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.github.alexgaard.mirror.test_utils.AsyncUtils.eventually;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.jupiter.api.Assertions.*;

public class PostgresEventCollectorTest {

    private static final DataSource dataSource = PostgresSingletonContainer.getDataSource();

    private static PostgresEventCollector collector;

    private static DataTypesRepository dataTypesRepository;

    private static final List<EventTransaction> collectedTransactions = new CopyOnWriteArrayList<>();

    @BeforeAll
    public static void setup() {
        dataTypesRepository = new DataTypesRepository(dataSource);

        DbUtils.initTables(dataSource);

        String name = "mirror_" + ((int) (Math.random() * 10_000));

        PgReplication pgReplication = new PgReplication()
                .replicationSlotName(name)
                .publicationName(name)
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

            assertEquals(18, dataChange.insertFields.size());

            assertEquals("id", dataChange.insertFields.get(0).name);
            assertNotNull(dataChange.insertFields.get(0).value);

            assertEquals("int2_field", dataChange.insertFields.get(1).name);
            assertEquals(dbo.int2_field, dataChange.insertFields.get(1).value);

            assertEquals("int4_field", dataChange.insertFields.get(2).name);
            assertEquals(dbo.int4_field, dataChange.insertFields.get(2).value);

            assertEquals("int8_field", dataChange.insertFields.get(3).name);
            assertEquals(dbo.int8_field, dataChange.insertFields.get(3).value);

            assertEquals("float4_field", dataChange.insertFields.get(4).name);
            assertEquals(dbo.float4_field, dataChange.insertFields.get(4).value);

            assertEquals("float8_field", dataChange.insertFields.get(5).name);
            assertEquals(dbo.float8_field, dataChange.insertFields.get(5).value);

            assertEquals("uuid_field", dataChange.insertFields.get(6).name);
            assertEquals(dbo.uuid_field, dataChange.insertFields.get(6).value);

            assertEquals("varchar_field", dataChange.insertFields.get(7).name);
            assertEquals(dbo.varchar_field, dataChange.insertFields.get(7).value);

            assertEquals("text_field", dataChange.insertFields.get(8).name);
            assertEquals(dbo.text_field, dataChange.insertFields.get(8).value);

            assertEquals("bool_field", dataChange.insertFields.get(9).name);
            assertEquals(dbo.bool_field, dataChange.insertFields.get(9).value);

            assertEquals("bytes_field", dataChange.insertFields.get(10).name);
            assertArrayEquals(dbo.bytes_field, (byte[]) dataChange.insertFields.get(10).value);

            assertEquals("char_field", dataChange.insertFields.get(11).name);
            assertEquals(dbo.char_field, dataChange.insertFields.get(11).value);

            assertEquals("json_field", dataChange.insertFields.get(12).name);
            assertEquals(dbo.json_field, dataChange.insertFields.get(12).value);

            assertEquals("jsonb_field", dataChange.insertFields.get(13).name);
            assertEquals(dbo.jsonb_field, dataChange.insertFields.get(13).value);

            assertEquals("date_field", dataChange.insertFields.get(14).name);
            assertEquals(dbo.date_field, dataChange.insertFields.get(14).value);

            assertEquals("time_field", dataChange.insertFields.get(15).name);
            assertEquals(dbo.time_field.truncatedTo(MILLIS), ((LocalTime) dataChange.insertFields.get(15).value).truncatedTo(MILLIS));

            assertEquals("timestamp_field", dataChange.insertFields.get(16).name);
            assertEquals(dbo.timestamp_field.truncatedTo(MILLIS), ((LocalDateTime) dataChange.insertFields.get(16).value).truncatedTo(MILLIS));

            assertEquals("timestamptz_field", dataChange.insertFields.get(17).name);
            assertEquals(dbo.timestamptz_field.truncatedTo(MILLIS), ((OffsetDateTime) dataChange.insertFields.get(17).value).truncatedTo(MILLIS));
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

    private void insertDataTypes(DataTypesDbo dbo) {
        String sql = "insert into data_types (" +
                "id," +
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
                ") values (?,?,?,?,?,?,?::uuid,?,?,?,?,?,?::json,?::jsonb,?,?,?,?)";

        QueryUtils.update(dataSource, sql, statement -> {
            if (dbo.id != null) {
                statement.setInt(1, dbo.id);
            } else {
                statement.setNull(1, Types.INTEGER);
            }

            if (dbo.int2_field != null) {
                statement.setShort(2, dbo.int2_field);
            } else {
                statement.setNull(2, Types.SMALLINT);
            }

            if (dbo.int4_field != null) {
                statement.setInt(3, dbo.int4_field);
            } else {
                statement.setNull(3, Types.INTEGER);
            }

            if (dbo.int8_field != null) {
                statement.setLong(4, dbo.int8_field);
            } else {
                statement.setNull(4, Types.BIGINT);
            }

            if (dbo.float4_field != null) {
                statement.setFloat(5, dbo.float4_field);
            } else {
                statement.setNull(5, Types.FLOAT);
            }

            if (dbo.float8_field != null) {
                statement.setDouble(6, dbo.float8_field);
            } else {
                statement.setNull(6, Types.DOUBLE);
            }

            if (dbo.uuid_field != null) {
                statement.setString(7, dbo.uuid_field.toString());
            } else {
                statement.setNull(7, Types.VARCHAR);
            }

            if (dbo.varchar_field != null) {
                statement.setString(8, dbo.varchar_field);
            } else {
                statement.setNull(8, Types.VARCHAR);
            }

            if (dbo.text_field != null) {
                statement.setString(9, dbo.text_field);
            } else {
                statement.setNull(9, Types.VARCHAR);
            }

            if (dbo.bool_field != null) {
                statement.setBoolean(10, dbo.bool_field);
            } else {
                statement.setNull(10, Types.BOOLEAN);
            }

            if (dbo.bytes_field != null) {
                statement.setBytes(11, dbo.bytes_field);
            } else {
                statement.setNull(11, Types.BINARY);
            }

            if (dbo.char_field != null) {
                statement.setString(12, dbo.char_field.toString());
            } else {
                statement.setNull(12, Types.CHAR);
            }

            if (dbo.json_field != null) {
                statement.setString(13, dbo.json_field);
            } else {
                statement.setNull(13, Types.VARCHAR);
            }

            if (dbo.jsonb_field != null) {
                statement.setString(14, dbo.jsonb_field);
            } else {
                statement.setNull(14, Types.VARCHAR);
            }

            if (dbo.date_field != null) {
                statement.setObject(15, dbo.date_field);
            } else {
                statement.setNull(15, Types.DATE);
            }

            if (dbo.time_field != null) {
                statement.setObject(16, dbo.time_field);
            } else {
                statement.setNull(16, Types.TIME);
            }

            if (dbo.timestamp_field != null) {
                statement.setObject(17, dbo.timestamp_field);
            } else {
                statement.setNull(17, Types.TIMESTAMP);
            }

            if (dbo.timestamptz_field != null) {
                statement.setObject(18, dbo.timestamptz_field);
            } else {
                statement.setNull(18, Types.TIME_WITH_TIMEZONE);
            }

            statement.executeUpdate();
        });
    }

    @Test
    public void should_handle_update_event() {
        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = 87;
        dbo.int2_field = 5;

        dataTypesRepository.insertDataTypes(dbo);

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

            assertEquals("id", updateEvent.updateFields.get(0).name);
            assertNotNull(updateEvent.updateFields.get(0).value);

            assertEquals("int2_field", updateEvent.updateFields.get(1).name);
            assertEquals(update.int2_field, updateEvent.updateFields.get(1).value);

            assertEquals("int4_field", updateEvent.updateFields.get(2).name);
            assertEquals(update.int4_field, updateEvent.updateFields.get(2).value);

            assertEquals("int8_field", updateEvent.updateFields.get(3).name);
            assertEquals(update.int8_field, updateEvent.updateFields.get(3).value);

            assertEquals("float4_field", updateEvent.updateFields.get(4).name);
            assertEquals(update.float4_field, updateEvent.updateFields.get(4).value);

            assertEquals("float8_field", updateEvent.updateFields.get(5).name);
            assertEquals(update.float8_field, updateEvent.updateFields.get(5).value);

            assertEquals("uuid_field", updateEvent.updateFields.get(6).name);
            assertEquals(update.uuid_field, updateEvent.updateFields.get(6).value);

            assertEquals("varchar_field", updateEvent.updateFields.get(7).name);
            assertEquals(update.varchar_field, updateEvent.updateFields.get(7).value);

            assertEquals("text_field", updateEvent.updateFields.get(8).name);
            assertEquals(update.text_field, updateEvent.updateFields.get(8).value);

            assertEquals("bool_field", updateEvent.updateFields.get(9).name);
            assertEquals(update.bool_field, updateEvent.updateFields.get(9).value);

            assertEquals("bytes_field", updateEvent.updateFields.get(10).name);
            assertArrayEquals(update.bytes_field, (byte[]) updateEvent.updateFields.get(10).value);

            assertEquals("char_field", updateEvent.updateFields.get(11).name);
            assertEquals(update.char_field, updateEvent.updateFields.get(11).value);

            assertEquals("json_field", updateEvent.updateFields.get(12).name);
            assertEquals(update.json_field, updateEvent.updateFields.get(12).value);

            assertEquals("jsonb_field", updateEvent.updateFields.get(13).name);
            assertEquals(update.jsonb_field, updateEvent.updateFields.get(13).value);

            assertEquals("date_field", updateEvent.updateFields.get(14).name);
            assertEquals(update.date_field, updateEvent.updateFields.get(14).value);

            assertEquals("time_field", updateEvent.updateFields.get(15).name);
            assertEquals(update.time_field.truncatedTo(MILLIS), ((LocalTime) updateEvent.updateFields.get(15).value).truncatedTo(MILLIS));

            assertEquals("timestamp_field", updateEvent.updateFields.get(16).name);
            assertEquals(update.timestamp_field.truncatedTo(MILLIS), ((LocalDateTime) updateEvent.updateFields.get(16).value).truncatedTo(MILLIS));

            assertEquals("timestamptz_field", updateEvent.updateFields.get(17).name);
            assertEquals(update.timestamptz_field.truncatedTo(MILLIS), ((OffsetDateTime) updateEvent.updateFields.get(17).value).truncatedTo(MILLIS));
        });
    }


}
