package com.github.alexgaard.mirror.rabbitmq;

import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.event.Event;
import com.github.alexgaard.mirror.core.event.EventTransaction;
import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.postgres.event.UpdateEvent;
import com.github.alexgaard.mirror.rabbitmq.receiver.RabbitMqReceiver;
import com.github.alexgaard.mirror.rabbitmq.sender.RabbitMqEventSender;
import com.github.alexgaard.mirror.test_utils.RabbitMqSingletonContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.alexgaard.mirror.postgres.event.json.JsonSerde.*;
import static com.github.alexgaard.mirror.test_utils.AsyncUtils.eventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RabbitMqSendReceiveTest {

    private static final String queue = "queue-" + UUID.randomUUID();

    private static final String exchange = "exchange-" + UUID.randomUUID();

    private static final String routingKey = "key-" + UUID.randomUUID();

    @BeforeAll
    public static void setup() {
        RabbitMqSingletonContainer.setupExchangeWithQueue(queue, exchange, routingKey);
    }

    @Test
    public void shouldSendAndReceiveMessage() {
        RabbitMqReceiver receiver = new RabbitMqReceiver(
                RabbitMqSingletonContainer.createConnectionFactory(),
                queue,
                jsonDeserializer
        );

        AtomicReference<EventTransaction> transactionRef = new AtomicReference<>();

        receiver.setOnTransactionReceived((transaction) -> {
            transactionRef.set(transaction);
            return Result.ok();
        });

        receiver.start();


        RabbitMqEventSender sender = new RabbitMqEventSender(RabbitMqSingletonContainer.createConnectionFactory(), exchange, routingKey, jsonSerializer);

        OffsetDateTime nowUtc = OffsetDateTime.now(ZoneId.of("UTC"));

        List<Event> events = List.of(
//                new InsertEvent(UUID.randomUUID(), "test", "my-table", 0, List.of(
//                        new Field("id", Field.Type.INT32, 5)
//                ), nowUtc),
                new UpdateEvent(UUID.randomUUID(), "test", "my-table", 0,
                        List.of(new Field("id", Field.Type.INT32, 5)),
                        List.of(new Field("name", Field.Type.TEXT, "hello")),
                nowUtc)
//                new DeleteEvent(UUID.randomUUID(), "test", "my-table", 0, List.of(
//                        new Field("id", Field.Type.INT32, 5)
//                ), nowUtc)
        );

        EventTransaction transaction = new EventTransaction(
                UUID.randomUUID(),
                "test",
                events,
                nowUtc,
                nowUtc
        );

        sender.send(transaction);

        eventually(() -> {
            assertNotNull(transactionRef.get());
            assertEquals(jsonMapper.writeValueAsString(transaction), jsonMapper.writeValueAsString(transactionRef.get()));
        });
    }

}
