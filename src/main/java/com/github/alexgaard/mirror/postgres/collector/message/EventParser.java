package com.github.alexgaard.mirror.postgres.collector.message;

public class EventParser {

    public static Message parseEvent(RawMessage event) {
        char messageType = (char) event.data[0];

        switch (messageType) {
            case 'B':
                return BeginMessage.parse(event);
            case 'C':
                return CommitMessage.parse(event);
            case 'I':
                return InsertMessage.parse(event);
            case 'R':
                return RelationMessage.parse(event);
            default:
                throw new IllegalArgumentException("Unknown message of type: " + messageType);
        }
    }

}
