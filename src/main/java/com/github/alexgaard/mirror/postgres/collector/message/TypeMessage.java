package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.core.exception.ParseException;
import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class TypeMessage extends Message {

    public static final char ID = 'Y';

    protected TypeMessage(String lsn, int xid) {
        super(lsn, xid, Type.TYPE);
    }

    /*
    Format:
        Byte1('Y')
        Identifies the message as a type message.

        Int32 (TransactionId)
        Xid of the transaction (only present for streamed transactions). This field is available since protocol version 2.

        Int32 (Oid)
        OID of the data type.

        String
        Namespace (empty string for pg_catalog).

        String
        Name of the data type.
    */

    public static TypeMessage parse(RawMessage msg) {
        PgoutputParser parser = new PgoutputParser(msg.data);

        char type = parser.nextChar();

        if (type != ID) {
            throw badMessageId(ID, type);
        }

        return new TypeMessage(msg.lsn, msg.xid);
    }

}
