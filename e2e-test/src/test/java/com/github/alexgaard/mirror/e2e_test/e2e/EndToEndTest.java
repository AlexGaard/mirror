package com.github.alexgaard.mirror.e2e_test.e2e;

import com.github.alexgaard.mirror.common_test.*;
import com.github.alexgaard.mirror.postgres.collector.PgReplication;
import com.github.alexgaard.mirror.postgres.collector.PostgresCollector;
import com.github.alexgaard.mirror.postgres.processor.PostgresProcessor;
import com.github.alexgaard.mirror.rabbitmq.RabbitMqReceiver;
import com.github.alexgaard.mirror.rabbitmq.RabbitMqSender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static com.github.alexgaard.mirror.common_test.AsyncUtils.eventually;
import static com.github.alexgaard.mirror.postgres_serde.JsonSerde.jsonDeserializer;
import static com.github.alexgaard.mirror.postgres_serde.JsonSerde.jsonSerializer;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EndToEndTest {

    private static final PostgresContainerWrapper container1 = new PostgresContainerWrapper();

    private static final PostgresContainerWrapper container2 = new PostgresContainerWrapper();

    private static DataSource dataSource1;

    private static DataSource dataSource2;

    @BeforeAll
    public static void setup() {
        container1.start();
        container2.start();

        dataSource1 = container1.getDataSource();
        dataSource2 = container2.getDataSource();

        DbUtils.initTables(dataSource1);
        DbUtils.initTables(dataSource2);

        RabbitMqSingletonContainer.setupExchangeWithQueue("my-queue-1", "my-exchange", "key-1");
        RabbitMqSingletonContainer.setupExchangeWithQueue("my-queue-2", "my-exchange", "key-2");
    }

    @AfterAll
    public static void cleanup() {
        container1.stop();
        container2.stop();
    }

    @Test
    public void should_propagate_events_from_one_database_to_another() {
        String name = "mirror_" + ((int) (Math.random() * 10_000));

        PgReplication pgReplication = new PgReplication()
                .replicationSlotName(name)
                .publicationName(name)
                .allTables();

        var collector1 = new PostgresCollector("test-1", dataSource1, Duration.ofMillis(100), pgReplication);
        var sender1 = new RabbitMqSender(RabbitMqSingletonContainer.createConnectionFactory(), "my-exchange", "key-2", jsonSerializer);

        collector1.setOnTransactionCollected(sender1::send);
        collector1.start();

        var receiver1 = new RabbitMqReceiver(RabbitMqSingletonContainer.createConnectionFactory(), "my-queue-1", jsonDeserializer);
        var processor1 = new PostgresProcessor(dataSource1);

        receiver1.setOnTransactionReceived(processor1::process);
        receiver1.start();

        // ========================

        var collector2 = new PostgresCollector("test-2", dataSource2, Duration.ofMillis(100), pgReplication);
        var sender2 = new RabbitMqSender(RabbitMqSingletonContainer.createConnectionFactory(), "my-exchange", "key-1", jsonSerializer);

        collector2.setOnTransactionCollected((sender2::send));
        collector2.start();

        var receiver2 = new RabbitMqReceiver(RabbitMqSingletonContainer.createConnectionFactory(), "my-queue-2", jsonDeserializer);
        var processor2 = new PostgresProcessor(dataSource2);

        receiver2.setOnTransactionReceived(processor2::process);
        receiver2.start();

        // ========================

        DataTypesRepository repo1 = new DataTypesRepository(dataSource1);
        DataTypesRepository repo2 = new DataTypesRepository(dataSource2);

        DataTypesDbo dbo1 = new DataTypesDbo();
        dbo1.id = 5;
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

        // ========================

        DataTypesDbo dbo2 = new DataTypesDbo();
        dbo2.id = 8;
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

}
