package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;
import com.github.alexgaard.mirror.test_utils.RawEventCreator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogicalDecodingMessageTest {

    @Test
    public void shouldParseRawEvent() {
        RawMessage rawMessage = RawEventCreator.create("4D0100000000016063F068656C6C6F0000000005776F726C64");

        LogicalDecodingMessage message = LogicalDecodingMessage.parse(rawMessage);

        assertTrue(message.isTransactional);
        assertEquals("hello", message.prefix);
        assertEquals("world", new String(message.content));
    }


}
