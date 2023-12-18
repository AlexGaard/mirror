package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class OriginMessage extends Message {

    public static final char ID = 'O';

    public final long originServerCommitLsn;

    public final String name;

    protected OriginMessage(String lsn, int xid, long originServerCommitLsn, String name) {
        super(lsn, xid, Type.ORIGIN);
        this.originServerCommitLsn = originServerCommitLsn;
        this.name = name;
    }

    /*
    Format:
        Byte1('O')
        Identifies the message as an origin message.

        Int64 (XLogRecPtr)
        The LSN of the commit on the origin server.

        String
        Name of the origin.

        Note that there can be multiple Origin messages inside a single transaction.
    */

    public static OriginMessage parse(RawMessage msg) {
        PgoutputParser parser = new PgoutputParser(msg.data);

        char type = parser.nextChar();

        if (type != ID) {
            throw badMessageId(ID, type);
        }

        long originServerCommitLsn = parser.nextLong();

        String name = parser.nextString();

        return new OriginMessage(msg.lsn, msg.xid, originServerCommitLsn, name);
    }
}
