package com.github.alexgaard.mirror.e2e_test.e2e;

import com.github.alexgaard.mirror.common_test.*;
import com.github.alexgaard.mirror.core.utils.ExceptionUtil;
import com.github.alexgaard.mirror.postgres.collector.PgReplication;
import com.github.alexgaard.mirror.postgres.collector.PostgresEventCollector;
import com.github.alexgaard.mirror.postgres.processor.PostgresEventProcessor;
import com.github.alexgaard.mirror.rabbitmq.RabbitMqEventReceiver;
import com.github.alexgaard.mirror.rabbitmq.RabbitMqEventSender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static com.github.alexgaard.mirror.common_test.AsyncUtils.eventually;
import static com.github.alexgaard.mirror.common_test.TestDataGenerator.*;
import static com.github.alexgaard.mirror.postgres_serde.JsonSerde.jsonDeserializer;
import static com.github.alexgaard.mirror.postgres_serde.JsonSerde.jsonSerializer;
import static org.junit.jupiter.api.Assertions.*;

public class EndToEndTest {

    private static final PostgresContainerWrapper container1 = new PostgresContainerWrapper();

    private static final PostgresContainerWrapper container2 = new PostgresContainerWrapper();

    private static DataSource dataSource1;

    private static DataSource dataSource2;

    private DataTypesRepository repo1;
    private DataTypesRepository repo2;

    private String name;

    private String exchange;

    private String queue1;
    private String queue2;

    private String key1;
    private String key2;

    private PostgresEventCollector collector1;
    private RabbitMqEventSender sender1;

    private RabbitMqEventReceiver receiver1;
    private PostgresEventProcessor processor1;

    private PostgresEventCollector collector2;
    private RabbitMqEventSender sender2;

    private RabbitMqEventReceiver receiver2;
    private PostgresEventProcessor processor2;

    
    @BeforeAll
    public static void init() {
        container1.start();
        container2.start();

        dataSource1 = container1.getDataSource();
        dataSource2 = container2.getDataSource();

        DbUtils.initTables(dataSource1);
        DbUtils.initTables(dataSource2);
    }
    
    @BeforeEach
    public void setup() {
        repo1 = new DataTypesRepository(dataSource1);
        repo2 = new DataTypesRepository(dataSource2);

        name = newReplicationName();

        exchange = newRabbitMqExchange();

        queue1 = newRabbitMqQueue();
        queue2 = newRabbitMqQueue();

        key1 = newRabbitMqKey();
        key2 = newRabbitMqKey();

        RabbitMqSingletonContainer.setupExchangeWithQueue(queue1, exchange, key1);
        RabbitMqSingletonContainer.setupExchangeWithQueue(queue2, exchange, key2);

        PgReplication pgReplication = new PgReplication()
                .replicationSlotName(name)
                .publicationName(name)
                .allTables();

        collector1 = new PostgresEventCollector("test-1", dataSource1, Duration.ofMillis(100), pgReplication);
        sender1 = new RabbitMqEventSender(RabbitMqSingletonContainer.createConnectionFactory(), exchange, key2, jsonSerializer);

        receiver1 = new RabbitMqEventReceiver(RabbitMqSingletonContainer.createConnectionFactory(), queue1, jsonDeserializer);
        processor1 = new PostgresEventProcessor(dataSource1);

        collector2 = new PostgresEventCollector("test-2", dataSource2, Duration.ofMillis(100), pgReplication);
        sender2 = new RabbitMqEventSender(RabbitMqSingletonContainer.createConnectionFactory(), exchange, key1, jsonSerializer);

        receiver2 = new RabbitMqEventReceiver(RabbitMqSingletonContainer.createConnectionFactory(), queue2, jsonDeserializer);
        processor2 = new PostgresEventProcessor(dataSource2);

        repo1.clear();
        repo2.clear();

        collector1.setEventSink(sender1);
        collector1.start();

        receiver1.setEventSink(processor1);
        receiver1.start();

        collector2.setEventSink((sender2));
        collector2.start();

        receiver2.setEventSink(processor2);
        receiver2.start();
    }

    @AfterAll
    public static void cleanup() {
        container1.stop();
        container2.stop();
    }

    @Test
    public void should_propagate_events_from_one_database_to_another() {
        DataTypesDbo dbo1 = new DataTypesDbo();
        dbo1.id = newId();
        dbo1.int2_field = 5;
        dbo1.int4_field = 100;
        dbo1.int8_field = 48L;
        dbo1.float4_field = 5.32f;
        dbo1.float8_field = 8932.43;
        dbo1.uuid_field = UUID.randomUUID();
        dbo1.varchar_field = "varchar";
        dbo1.text_field = "text";
        dbo1.bool_field = true;
        dbo1.bytes_field = new byte[]{5, 6, 9};
        dbo1.char_field = 'C';
        dbo1.json_field = "{\"json\": true}";
        dbo1.jsonb_field = "{\"json\": true}";
        dbo1.date_field = LocalDate.now();
        dbo1.time_field = LocalTime.now().truncatedTo(ChronoUnit.MILLIS);
        dbo1.timestamp_field = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        dbo1.timestamptz_field = OffsetDateTime.now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.MILLIS);

        repo1.insertDataTypes(dbo1);

        eventually(() -> {
            DataTypesDbo dboFrom2 = repo2.getDataTypes(dbo1.id).orElseThrow();
            assertEquals(dbo1, dboFrom2);
        });

        DataTypesDbo dbo2 = new DataTypesDbo();
        dbo2.id = newId();
        dbo2.int2_field = 12;
        dbo2.int4_field = null;
        dbo2.int8_field = 42L;
        dbo2.float4_field = null;
        dbo2.float8_field = 0.1;
        dbo2.uuid_field = null;
        dbo2.varchar_field = "test";
        dbo2.text_field = "text";
        dbo2.bool_field = true;
        dbo2.bytes_field = null;
        dbo2.char_field = 'N';
        dbo2.json_field = null;
        dbo2.jsonb_field = "{\"json\": true}";
        dbo2.date_field = LocalDate.now();
        dbo2.time_field = LocalTime.now().truncatedTo(ChronoUnit.MILLIS);
        dbo2.timestamp_field = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        dbo2.timestamptz_field = OffsetDateTime.now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.MILLIS);

        repo2.insertDataTypes(dbo2);

        eventually(() -> {
            DataTypesDbo dboFrom1 = repo1.getDataTypes(dbo2.id).orElseThrow();
            assertEquals(dbo2, dboFrom1);
        });
    }

    @Test
    public void should_send_receive_delete_event() {
        DataTypesDbo dbo1 = new DataTypesDbo();
        dbo1.id = newId();

        repo1.insertDataTypes(dbo1);

        eventually(() -> assertNotNull(repo2.getDataTypes(dbo1.id)));

        repo1.deleteDataTypeRow(dbo1.id);

        eventually(() -> assertNull(repo2.getDataTypes(dbo1.id)));
    }

    @Test
    public void should_send_and_receive_many_events() {
        int events = 500;

        for (int i = 0; i < events; i++) {
            DataTypesDbo dbo1 = new DataTypesDbo();
            dbo1.id = newId();

            repo1.insertDataTypes(dbo1);
        }

        eventually(Duration.ofSeconds(30), () -> {
            int count = repo2.getDataTypesCount();
            assertEquals(events, count);
        });
    }

}
