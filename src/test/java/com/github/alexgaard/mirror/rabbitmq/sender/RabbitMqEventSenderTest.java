package com.github.alexgaard.mirror.rabbitmq.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.alexgaard.mirror.core.event.EventTransaction;
import com.github.alexgaard.mirror.core.serde.Serializer;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class RabbitMqEventSenderTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private final Serializer serializer = mapper::writeValueAsString;

    @Test
    public void test() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("rabbit");
        factory.setPassword("qwerty");

        RabbitMqEventSender sender = new RabbitMqEventSender(factory, "testExchange", "testQueue", serializer);

        for (int i = 0; i < 1000; i++) {
            sender.send(EventTransaction .of("source", Collections.emptyList()));
        }
    }

}
