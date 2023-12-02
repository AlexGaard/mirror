package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;
import com.github.alexgaard.mirror.test_utils.RawEventCreator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeleteMessageTest {

    @Test
    public void shouldParseRawEvent() {
        RawMessage rawMessage = RawEventCreator.create("440000400D4B00047400000001316E6E6E");

        DeleteMessage event = DeleteMessage.parse(rawMessage);

        assertEquals(16397, event.relationMessageOid);
        assertEquals('K', event.replicaIdentity);
        assertEquals(4, event.columns.size());
        assertEquals(TupleDataColumn.Type.TEXT, event.columns.get(0).type);
        assertEquals("1", event.columns.get(0).getData());
    }

}
