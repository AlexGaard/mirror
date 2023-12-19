package com.github.alexgaard.mirror.rabbitmq;

import com.github.alexgaard.mirror.core.Receiver;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.event.EventTransactionConsumer;
import com.github.alexgaard.mirror.core.event.EventTransaction;
import com.github.alexgaard.mirror.core.serde.Deserializer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.runWithResult;
import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;


public class RabbitMqReceiver implements Receiver {

    private final static Logger log = LoggerFactory.getLogger(RabbitMqReceiver.class);

    private final ConnectionFactory factory;
    private final String queueName;

    private final Deserializer deserializer;

    private EventTransactionConsumer onEventReceived;

    private volatile Connection connection;

    private volatile Channel channel;

    public RabbitMqReceiver(ConnectionFactory factory, String queueName, Deserializer deserializer) {
        this.factory = factory;
        this.queueName = queueName;
        this.deserializer = deserializer;
    }

    @Override
    public void setOnTransactionReceived(EventTransactionConsumer onEventReceived) {
        this.onEventReceived = onEventReceived;
    }

    @Override
    public void start() {
        if (connection == null || !connection.isOpen()) {
            try {
                connection = factory.newConnection();
            } catch (IOException | TimeoutException e) {
                log.error("Unable to open connection", e);
                throw softenException(e);
            }
        }

        if (channel == null || !channel.isOpen()) {
            try {
                channel = connection.createChannel();
            } catch (IOException e) {
                log.error("Unable to open channel", e);
                throw softenException(e);
            }
        }

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            Result result = runWithResult(() -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

                EventTransaction transaction = deserializer.deserialize(message);

                return onEventReceived.consume(transaction);
            });

            long tag = delivery.getEnvelope().getDeliveryTag();

            if (result.isOk()) {
                channel.basicAck(tag, false);
            } else {
                log.error("Failed to process transaction", result.getError().get());
                channel.basicNack(tag, false, true);
            }
        };

        try {
            channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {});
        } catch (IOException e) {
            throw softenException(e);
        }
    }

    @Override
    public void stop() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (TimeoutException | IOException e) {
                log.error("Failed while closing RabbitMq receiver channel", e);
            }
        }

        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
            } catch (IOException e) {
                log.error("Failed while closing RabbitMq receiver connection", e);
            }
        }
    }
}
