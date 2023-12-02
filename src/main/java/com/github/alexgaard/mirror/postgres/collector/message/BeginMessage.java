package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.core.exception.ParseException;
import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;

public class BeginMessage extends Message {

    public final long transactionLsn;

    public final long timestamp;

    public BeginMessage(String lsn, int xid, long transactionLsn, long timestamp) {
        super(lsn, xid, Type.BEGIN);
        this.transactionLsn = transactionLsn;
        this.timestamp = timestamp;
    }

    /*
        Format:
            Byte1('B')
            Identifies the message as a begin message.

            Int64 (XLogRecPtr)
            The final LSN of the transaction.

            Int64 (TimestampTz)
            Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).

            Int32 (TransactionId)
            Xid of the transaction.
    */
    public static BeginMessage parse(RawMessage event) {
        PgoutputParser parser = new PgoutputParser(event.data);

        char type = parser.nextChar();

        if (type != 'B') {
            throw new ParseException("Bad type: " + type);
        }

        long transactionLsn = parser.nextLong();
        long timestamp = parser.nextLong();
        int xid = parser.nextInt();

        return new BeginMessage(event.lsn, xid, transactionLsn, timestamp);
    }

}
