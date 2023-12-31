package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.test_utils.RawEventCreator;
import org.junit.jupiter.api.Test;

import static junit.framework.TestCase.assertEquals;

public class BeginMessageTest {

    @Test
    public void shouldParseRawEvent() {
        RawMessage rawMessage = RawEventCreator.create("42000000000158A6800002ACF61ADF0BBE000002F6");

        BeginMessage message = BeginMessage.parse(rawMessage);

        assertEquals(753122966178750L, message.timestamp);
    }

}
