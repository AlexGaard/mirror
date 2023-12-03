package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.test_utils.RawEventCreator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class RelationMessageTest {

    @Test
    public void shouldParseRawEvent() {
        // Insert single column with value of 42
        RawMessage rawMessage = RawEventCreator.create("52000040157075626C696300706572736F6E32006400010169640000000017FFFFFFFF");

        RelationMessage message = RelationMessage.parse(rawMessage);

        assertEquals(16405, message.oid);
        assertEquals("public", message.namespace);
        assertEquals("person2", message.relationName);
        assertEquals(1, message.columns.size());
        assertEquals(23, message.columns.get(0).dataOid);
        assertEquals(-1, message.columns.get(0).typeModifier);
        assertEquals("id", message.columns.get(0).name);
        assertTrue(message.columns.get(0).partOfKey);
    }

}
