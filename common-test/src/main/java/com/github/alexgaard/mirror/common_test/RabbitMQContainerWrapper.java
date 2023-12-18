package com.github.alexgaard.mirror.common_test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.testcontainers.containers.RabbitMQContainer;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;

public class RabbitMQContainerWrapper {

    private static final String RABBIT_MQ_IMAGE = "rabbitmq:3.7.25-management-alpine";

    public static RabbitMQContainer rabbitMq = new RabbitMQContainer(RABBIT_MQ_IMAGE);

    public void start() {
        rabbitMq.start();
    }

    public void stop() {
        rabbitMq.stop();
    }

    public ConnectionFactory createConnectionFactory() {
        if (!rabbitMq.isRunning()) {
            throw new IllegalStateException("Container has not been started!");
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername(rabbitMq.getAdminUsername());
        factory.setPassword(rabbitMq.getAdminPassword());
        factory.setPort(rabbitMq.getAmqpPort());

        return factory;
    }

    public void setupExchangeWithQueue(String queue, String exchange, String routingKey) {
        withChannel((channel) -> {
            channel.exchangeDeclare(exchange, "direct");
            channel.queueDeclare(queue, true, false, false, null);
            channel.queueBind(queue, exchange, routingKey);
        });
    }

    private void withChannel(UnsafeConsumer<Channel> channelConsumer) {
        ConnectionFactory factory = createConnectionFactory();

        try (var connection = factory.newConnection(); var channel = connection.createChannel()) {
            channelConsumer.accept(channel);
        } catch (Exception e) {
           throw softenException(e);
        }
    }

    interface UnsafeConsumer<T> {
        void accept(T var1) throws Exception;
    }

}
