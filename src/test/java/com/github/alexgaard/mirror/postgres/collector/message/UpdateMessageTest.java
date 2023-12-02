package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;
import com.github.alexgaard.mirror.test_utils.RawEventCreator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UpdateMessageTest {

    @Test
    public void shouldParseRawEvent() {
        RawMessage rawMessage = RawEventCreator.create("550000C0014E0012740000000234337400000001386E6E6E6E6E6E6E7400000001666E6E6E6E6E6E6E6E");

        UpdateMessage event = UpdateMessage.parse(rawMessage);

        assertEquals(49153, event.relationMessageOid);
        assertNull(event.replicaIdentityType);
        assertTrue(event.identifyingColumns.isEmpty());
        assertEquals(18, event.updatedColumns.size());
        assertEquals("43", event.updatedColumns.get(0).getData());
        assertEquals("8", event.updatedColumns.get(1).getData());
        assertEquals("f", event.updatedColumns.get(9).getData());
    }

}
