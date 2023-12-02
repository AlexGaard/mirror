package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.test_utils.RawEventCreator;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class InsertMessageTest {

    @Test
    public void shouldParseRawEvent() {
        // Insert single column with value of 42
        RawMessage rawMessage = RawEventCreator.create("49000040154E000174000000023432");

        InsertMessage event = InsertMessage.parse(rawMessage);

        assertEquals(16405, event.relationMessageOid);
        assertEquals(1, event.columns.size());
        assertEquals(TupleDataColumn.Type.TEXT, event.columns.get(0).type);
        assertEquals("42", event.columns.get(0).getData());
        assertEquals("42", event.columns.get(0).getData());
    }

}
