package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;

import java.util.ArrayList;
import java.util.List;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class TruncateMessage extends Message {

    public enum Option {
        CASCADE,
        RESTART_IDENTITY
    }

    public static final char ID = 'T';

    public final Option option;

    public final List<Integer> relationMessageOids;

    protected TruncateMessage(String lsn, int xid, Option option, List<Integer> relationMessageOids) {
        super(lsn, xid, Type.TRUNCATE);
        this.option = option;
        this.relationMessageOids = relationMessageOids;
    }

    /*
    Format:
        Byte1('T')
        Identifies the message as a truncate message.

        Int32 (TransactionId)
        Xid of the transaction (only present for streamed transactions). This field is available since protocol version 2.

        Int32
        Number of relations

        Int8
        Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY

        Int32 (Oid)
        OID of the relation corresponding to the ID in the relation message. This field is repeated for each relation.
    */

    public static TruncateMessage parse(RawMessage msg) {
        PgoutputParser parser = new PgoutputParser(msg.data);

        char type = parser.nextChar();

        if (type != ID) {
            throw badMessageId(ID, type);
        }

        int relationCount = parser.nextInt();

        byte optionByte = parser.nextByte();
        Option option = optionByte == 1 ? Option.CASCADE : Option.RESTART_IDENTITY;

        List<Integer> relationMessageOids = new ArrayList<>(relationCount);

        for (int i = 0; i < relationCount; i++) {
            relationMessageOids.add(parser.nextInt());
        }

        return new TruncateMessage(msg.lsn, msg.xid, option, relationMessageOids);
    }

}
