package com.github.alexgaard.mirror.core;

import com.github.alexgaard.mirror.core.event.EventTransactionConsumer;

public interface Receiver {

    void setOnTransactionReceived(EventTransactionConsumer onEventReceived);

    void start();

    void stop();

}
