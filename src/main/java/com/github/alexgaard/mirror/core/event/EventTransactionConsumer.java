package com.github.alexgaard.mirror.core.event;

import com.github.alexgaard.mirror.core.Result;

public interface EventTransactionConsumer {

    Result consume(EventTransaction transaction);

}
