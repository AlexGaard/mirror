package com.github.alexgaard.mirror.postgres.processor.config;

import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.postgres.event.CustomMessageEvent;

import java.sql.Connection;

public interface CustomMessageHandler {

    Result handle(CustomMessageEvent customMessageEvent, Connection ongoingTransaction) throws Exception;

}
