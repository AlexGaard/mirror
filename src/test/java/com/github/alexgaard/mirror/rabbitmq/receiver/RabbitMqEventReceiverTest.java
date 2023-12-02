package com.github.alexgaard.mirror.rabbitmq.receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.alexgaard.mirror.core.event.EventTransaction;
import com.github.alexgaard.mirror.core.serde.Deserializer;
import com.github.alexgaard.mirror.core.serde.Serializer;
import com.github.alexgaard.mirror.rabbitmq.sender.RabbitMqEventSender;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.checkerframework.checker.units.qual.A;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexgaard.mirror.test_utils.AsyncUtils.eventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RabbitMqEventReceiverTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private final Deserializer deserializer = (data -> mapper.readValue(data, EventTransaction.class));

    @Test
    public void test() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("rabbit");
        factory.setPassword("qwerty");

        RabbitMqEventReceiver receiver = new RabbitMqEventReceiver(factory, "testQueue", deserializer);

        AtomicInteger counter = new AtomicInteger();

        receiver.initialize((transaction) -> {
            counter.incrementAndGet();
            System.out.println(transaction);
        });

        receiver.start();

        eventually(() -> {
            assertTrue(counter.get() >= 10);
        });
    }

}
