package com.github.alexgaard.mirror.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.alexgaard.mirror.common_test.AsyncUtils;
import com.github.alexgaard.mirror.common_test.RabbitMqSingletonContainer;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.Event;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.alexgaard.mirror.common_test.AsyncUtils.eventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RabbitMqSendReceiveTest {

    private static final String queue = "queue-" + UUID.randomUUID();

    private static final String exchange = "exchange-" + UUID.randomUUID();

    private static final String routingKey = "key-" + UUID.randomUUID();

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @BeforeAll
    public static void setup() {
        RabbitMqSingletonContainer.setupExchangeWithQueue(queue, exchange, routingKey);
    }

    @Test
    public void shouldSendAndReceiveMessage() {
        RabbitMqEventReceiver receiver = new RabbitMqEventReceiver(
                RabbitMqSingletonContainer.createConnectionFactory(),
                queue,
                (data) -> objectMapper.readValue(data, Event.class)
        );

        AtomicReference<Event> transactionRef = new AtomicReference<>();

        receiver.setEventSink((transaction) -> {
            transactionRef.set(transaction);
            return Result.ok();
        });

        receiver.start();


        RabbitMqEventSender sender = new RabbitMqEventSender(
                RabbitMqSingletonContainer.createConnectionFactory(),
                exchange,
                routingKey,
                objectMapper::writeValueAsBytes
        );

        OffsetDateTime nowUtc = OffsetDateTime.now(ZoneId.of("UTC"));

        Event event = new Event(
                UUID.randomUUID(),
                "test-src",
                "test",
                nowUtc
        );

        sender.consume(event);

        eventually(() -> {
            assertNotNull(transactionRef.get());
            assertEquals(objectMapper.writeValueAsString(event), objectMapper.writeValueAsString(transactionRef.get()));
        });
    }

}
