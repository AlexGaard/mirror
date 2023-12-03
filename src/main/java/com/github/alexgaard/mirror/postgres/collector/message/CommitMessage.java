package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class CommitMessage extends Message {

    public static final char ID = 'C';

    public final long commitLsn;

    public final long transactionEndLsn;

    public final long commitTimestamp;

    protected CommitMessage(String lsn, int xid, long commitLsn, long transactionEndLsn, long commitTimestamp) {
        super(lsn, xid, Type.COMMIT);
        this.commitLsn = commitLsn;
        this.transactionEndLsn = transactionEndLsn;
        this.commitTimestamp = commitTimestamp;
    }

    /*
    Format:
        Byte1('C')
        Identifies the message as a commit message.

        Int8(0)
        Flags; currently unused.

        Int64 (XLogRecPtr)
        The LSN of the commit.

        Int64 (XLogRecPtr)
        The end LSN of the transaction.

        Int64 (TimestampTz)
        Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    */

    public static CommitMessage parse(RawMessage msg) {
        PgoutputParser parser = new PgoutputParser(msg.data);

        char type = parser.nextChar();

        if (type != ID) {
            throw badMessageId(ID, type);
        }

        // Skip unused flags
        parser.nextByte();

        long commitLsn = parser.nextLong();

        long transactionEndLsn = parser.nextLong();

        long commitTimestamp = parser.nextLong();

        return new CommitMessage(msg.lsn, msg.xid, commitLsn, transactionEndLsn, commitTimestamp);
    }

}
