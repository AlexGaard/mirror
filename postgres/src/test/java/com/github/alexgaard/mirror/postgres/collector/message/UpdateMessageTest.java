package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.test_utils.RawEventCreator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UpdateMessageTest {

    @Test
    public void shouldParseRawEvent() {
        RawMessage rawMessage = RawEventCreator.create("550000C0014E0012740000000234337400000001386E6E6E6E6E6E6E7400000001666E6E6E6E6E6E6E6E");

        UpdateMessage message = UpdateMessage.parse(rawMessage);

        assertEquals(49153, message.relationMessageOid);
        assertNull(message.replicaIdentityType);
        assertTrue(message.oldTupleOrPkColumns.isEmpty());
        assertEquals(18, message.columnsAfterUpdate.size());
        assertEquals("43", message.columnsAfterUpdate.get(0).getData());
        assertEquals("8", message.columnsAfterUpdate.get(1).getData());
        assertEquals("f", message.columnsAfterUpdate.get(9).getData());
    }

}
