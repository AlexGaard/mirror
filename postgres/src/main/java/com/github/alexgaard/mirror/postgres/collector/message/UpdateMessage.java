package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;

import java.util.Collections;
import java.util.List;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class UpdateMessage extends Message {

    public static final char ID = 'U';

    public final int relationMessageOid;

    public final Character replicaIdentityType;

    public final List<TupleDataColumn> oldTupleOrPkColumns;

    public final List<TupleDataColumn> columnsAfterUpdate;

    protected UpdateMessage(
            String lsn,
            int xid,
            int relationMessageOid,
            Character replicaIdentityType,
            List<TupleDataColumn> oldTupleOrPkColumns,
            List<TupleDataColumn> columnsAfterUpdate
    ) {
        super(lsn, xid, Type.UPDATE);
        this.relationMessageOid = relationMessageOid;
        this.replicaIdentityType = replicaIdentityType;
        this.oldTupleOrPkColumns = oldTupleOrPkColumns;
        this.columnsAfterUpdate = columnsAfterUpdate;
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

            parser.nextChar(); // skip char 'N'
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateMessage that = (UpdateMessage) o;

        if (relationMessageOid != that.relationMessageOid) return false;
        if (!replicaIdentityType.equals(that.replicaIdentityType)) return false;
        if (!oldTupleOrPkColumns.equals(that.oldTupleOrPkColumns)) return false;
        return columnsAfterUpdate.equals(that.columnsAfterUpdate);
    }

    @Override
    public int hashCode() {
        int result = relationMessageOid;
        result = 31 * result + replicaIdentityType.hashCode();
        result = 31 * result + oldTupleOrPkColumns.hashCode();
        result = 31 * result + columnsAfterUpdate.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "UpdateMessage{" +
                "relationMessageOid=" + relationMessageOid +
                ", replicaIdentityType=" + replicaIdentityType +
                ", oldTupleOrPkColumns=" + oldTupleOrPkColumns +
                ", columnsAfterUpdate=" + columnsAfterUpdate +
                ", lsn='" + lsn + '\'' +
                ", xid=" + xid +
                ", type=" + type +
                '}';
    }
}
