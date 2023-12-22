package com.github.alexgaard.mirror.core.utils;

import com.github.alexgaard.mirror.core.EventSink;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.EventTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventLogger implements EventSink {

    private final static Logger log = LoggerFactory.getLogger(EventLogger.class);

    @Override
    public Result consume(EventTransaction transaction) {
        log.info("Event transaction: {}", transaction);
        return Result.ok();
    }

}
