package com.github.alexgaard.mirror.core.utils;

import com.github.alexgaard.mirror.core.Processor;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.event.EventTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventLogger implements Processor {

    private final static Logger log = LoggerFactory.getLogger(EventLogger.class);

    @Override
    public Result process(EventTransaction transaction) {
        log.info("Event transaction: {}", transaction);
        return Result.ok();
    }

}
