package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.test_utils.RawEventCreator;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class InsertMessageTest {

    @Test
    public void shouldParseRawEvent() {
        RawMessage rawMessage = RawEventCreator.create("49000040154E000174000000023432");

        InsertMessage message = InsertMessage.parse(rawMessage);

        assertEquals(16405, message.relationMessageOid);
        assertEquals(1, message.columns.size());
        assertEquals(TupleDataColumn.Type.TEXT, message.columns.get(0).type);
        assertEquals("42", message.columns.get(0).getData());
        assertEquals("42", message.columns.get(0).getData());
    }

}
