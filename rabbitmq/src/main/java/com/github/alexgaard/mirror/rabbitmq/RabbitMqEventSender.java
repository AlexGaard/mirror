package com.github.alexgaard.mirror.rabbitmq;

import com.github.alexgaard.mirror.core.EventSink;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.Event;
import com.github.alexgaard.mirror.core.serde.Serializer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMqEventSender implements EventSink {

    private final static Logger log = LoggerFactory.getLogger(RabbitMqEventSender.class);

    private final ConnectionFactory factory;
    private final String exchangeName;
    private final String routingKey;

    private final Serializer serializer;

    private Connection connection;

    private Channel channel;

    public RabbitMqEventSender(ConnectionFactory factory, String exchangeName, String routingKey, Serializer serializer) {
        this.factory = factory;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
        this.serializer = serializer;
    }

    @Override
    public synchronized Result consume(Event transaction) {
        if (connection == null || !connection.isOpen()) {
            try {
                connection = factory.newConnection();
            } catch (IOException | TimeoutException e) {
                log.error("Failed to send transaction {}. Unable to open connection", transaction.id, e);
                return Result.error(e);
            }
        }

        if (channel == null || !channel.isOpen()) {
            try {
                channel = connection.createChannel();
            } catch (IOException e) {
                log.error("Failed to send transaction {}. Unable to open channel", transaction.id, e);
                return Result.error(e);
            }
        }

        try {
            channel.basicPublish(exchangeName, routingKey, null, serializer.serialize(transaction));

            return Result.ok();
        } catch (IOException e) {
            return Result.error(e);
        }
    }

    public synchronized void close() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (TimeoutException | IOException e) {
                log.error("Failed while closing RabbitMq sender channel", e);
            }
        }

        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
            } catch (IOException e) {
               log.error("Failed while closing RabbitMq sender connection", e);
            }
        }
    }

}
