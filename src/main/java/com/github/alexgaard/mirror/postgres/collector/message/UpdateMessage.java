package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.core.exception.ParseException;
import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;

import java.util.Collections;
import java.util.List;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class UpdateMessage extends Message {

    public static final char ID = 'U';

    public final int relationMessageOid;

    public final Character replicaIdentityType;

    public final List<TupleDataColumn> identifyingColumns;

    public final List<TupleDataColumn> updatedColumns;

    protected UpdateMessage(
            String lsn,
            int xid,
            int relationMessageOid,
            Character replicaIdentityType,
            List<TupleDataColumn> identifyingColumns,
            List<TupleDataColumn> updatedColumns
    ) {
        super(lsn, xid, Type.UPDATE);
        this.relationMessageOid = relationMessageOid;
        this.replicaIdentityType = replicaIdentityType;
        this.identifyingColumns = identifyingColumns;
        this.updatedColumns = updatedColumns;
    }

    /*
    Format:
        Byte1('U')
        Identifies the message as an update message.

        Int32 (TransactionId)
        Xid of the transaction (only present for streamed transactions). This field is available since protocol version 2.

        Int32 (Oid)
        OID of the relation corresponding to the ID in the relation message.

        Byte1('K')
        Identifies the following TupleData submessage as a key. This field is optional and is only present if the update changed data in any of the column(s) that are part of the REPLICA IDENTITY index.

        Byte1('O')
        Identifies the following TupleData submessage as an old tuple. This field is optional and is only present if table in which the update happened has REPLICA IDENTITY set to FULL.

        TupleData
        TupleData message part representing the contents of the old tuple or primary key. Only present if the previous 'O' or 'K' part is present.

        Byte1('N')
        Identifies the following TupleData message as a new tuple.

        TupleData
        TupleData message part representing the contents of a new tuple.

        The Update message may contain either a 'K' message part or an 'O' message part or neither of them, but never both of them.
    */

    public static UpdateMessage parse(RawMessage msg) {
        PgoutputParser parser = new PgoutputParser(msg.data);

        char type = parser.nextChar();

        if (type != ID) {
            throw badMessageId(ID, type);
        }

        int relationOid = parser.nextInt();

        Character replicaIdentityType = replicaIdentityType(parser.nextChar());

        List<TupleDataColumn> identifyingColumns = Collections.emptyList();

        if (replicaIdentityType != null) {
            identifyingColumns = parser.nextTupleData();
        }

        List<TupleDataColumn> updatedColumns = parser.nextTupleData();

        return new UpdateMessage(
                msg.lsn,
                msg.xid,
                relationOid,
                replicaIdentityType,
                identifyingColumns,
                updatedColumns
        );
    }

    private static Character replicaIdentityType(char maybeReplicaIdentityType) {
        return maybeReplicaIdentityType == 'K' || maybeReplicaIdentityType == 'O'
                ? maybeReplicaIdentityType
                : null;
    }

}