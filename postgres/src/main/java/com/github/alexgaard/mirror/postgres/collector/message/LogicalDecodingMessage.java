package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class LogicalDecodingMessage extends Message {

    public static final char ID = 'M';

    public final boolean isTransactional;

    public final long lsn;

    public final String prefix;

    public final byte[] content;

    protected LogicalDecodingMessage(String lsn, int xid, boolean isTransactional, long lsn1, String prefix, byte[] content) {
        super(lsn, xid, Type.MESSAGE);
        this.isTransactional = isTransactional;
        this.lsn = lsn1;
        this.prefix = prefix;
        this.content = content;
    }

    /*
    Format:
        Byte1('M')
        Identifies the message as a logical decoding message.

        Int32 (TransactionId)
        Xid of the transaction (only present for streamed transactions). This field is available since protocol version 2.

        Int8
        Flags; Either 0 for no flags or 1 if the logical decoding message is transactional.

        Int64 (XLogRecPtr)
        The LSN of the logical decoding message.

        String
        The prefix of the logical decoding message.

        Int32
        Length of the content.

        Byten
        The content of the logical decoding message.
    */

    public static LogicalDecodingMessage parse(RawMessage msg) {
        PgoutputParser parser = new PgoutputParser(msg.data);

        char type = parser.nextChar();

        if (type != ID) {
            throw badMessageId(ID, type);
        }

        byte flags = parser.nextByte();

        long lsn = parser.nextLong();

        String prefix = parser.nextString();

        int contentLength = parser.nextInt();

        byte[] content = parser.nextBytes(contentLength);

        return new LogicalDecodingMessage(msg.lsn, msg.xid, flags == 1, lsn, prefix, content);
    }


}
