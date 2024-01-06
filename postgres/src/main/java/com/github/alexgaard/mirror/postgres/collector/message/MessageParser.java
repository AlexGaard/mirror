package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.core.exception.ParseException;

import static java.lang.String.format;

public class MessageParser {

    public static Message parse(RawMessage message) {
        if (message.data == null || message.data.length == 0) {
            throw new IllegalArgumentException("Message data is missing");
        }

        char messageType = (char) message.data[0];

        switch (messageType) {
            case BeginMessage.ID:
                return BeginMessage.parse(message);
            case CommitMessage.ID:
                return CommitMessage.parse(message);
            case InsertMessage.ID:
                return InsertMessage.parse(message);
            case RelationMessage.ID:
                return RelationMessage.parse(message);
            case DeleteMessage.ID:
                return DeleteMessage.parse(message);
            case CustomMessage.ID:
                return CustomMessage.parse(message);
            case TypeMessage.ID:
                return TypeMessage.parse(message);
            case TruncateMessage.ID:
                return TruncateMessage.parse(message);
            case OriginMessage.ID:
                return OriginMessage.parse(message);
            case UpdateMessage.ID:
                return UpdateMessage.parse(message);
            default:
                return null;
        }
    }

    protected static ParseException badMessageId(char expected, char actual) {
        return new ParseException(format("Expected message to start with id '%s', but was '%s'", expected, actual));
    }

}
