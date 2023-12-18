package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.test_utils.RawEventCreator;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeleteMessageTest {

    @Test
    public void shouldParseRawEvent() {
        RawMessage rawMessage = RawEventCreator.create("440000400D4B00047400000001316E6E6E");

        DeleteMessage message = DeleteMessage.parse(rawMessage);

        assertEquals(16397, message.relationMessageOid);
        assertEquals('K', message.replicaIdentity);
        assertEquals(4, message.columns.size());
        assertEquals(TupleDataColumn.Type.TEXT, message.columns.get(0).type);
        assertEquals("1", message.columns.get(0).getData());
    }

}
