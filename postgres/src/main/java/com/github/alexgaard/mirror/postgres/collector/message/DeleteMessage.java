package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.core.exception.ParseException;
import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;

import java.util.List;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class DeleteMessage extends Message {

    public static final char ID = 'D';

    public final int relationMessageOid;

    public final char replicaIdentity;

    public final List<TupleDataColumn> columns;

    protected DeleteMessage(String lsn, int xid, int relationMessageOid, char replicaIdentity, List<TupleDataColumn> columns) {
        super(lsn, xid, Type.DELETE);
        this.relationMessageOid = relationMessageOid;
        this.replicaIdentity = replicaIdentity;
        this.columns = columns;
    }

    /*
    Format:
        Byte1('D')
        Identifies the message as a delete message.

        Int32 (TransactionId)
        Xid of the transaction (only present for streamed transactions). This field is available since protocol version 2.

        Int32 (Oid)
        OID of the relation corresponding to the ID in the relation message.

        Byte1('K')
        Identifies the following TupleData submessage as a key. This field is present if the table in which the delete has happened uses an index as REPLICA IDENTITY.

        Byte1('O')
        Identifies the following TupleData message as an old tuple. This field is present if the table in which the delete happened has REPLICA IDENTITY set to FULL.

        TupleData
        TupleData message part representing the contents of the old tuple or primary key, depending on the previous field.

        The Delete message may contain either a 'K' message part or an 'O' message part, but never both of them.
     */

    public static DeleteMessage parse(RawMessage msg) {
        PgoutputParser parser = new PgoutputParser(msg.data);

        char type = parser.nextChar();

        if (type != ID) {
            throw badMessageId(ID, type);
        }

        int relationOid = parser.nextInt();

        char replicaIdentityType = parser.nextChar();

        List<TupleDataColumn> columns = parser.nextTupleData();

        return new DeleteMessage(msg.lsn, msg.xid, relationOid, replicaIdentityType, columns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeleteMessage that = (DeleteMessage) o;

        if (relationMessageOid != that.relationMessageOid) return false;
        if (replicaIdentity != that.replicaIdentity) return false;
        return columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        int result = relationMessageOid;
        result = 31 * result + (int) replicaIdentity;
        result = 31 * result + columns.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DeleteMessage{" +
                "relationMessageOid=" + relationMessageOid +
                ", replicaIdentity=" + replicaIdentity +
                ", columns=" + columns +
                ", lsn='" + lsn + '\'' +
                ", xid=" + xid +
                ", type=" + type +
                '}';
    }
}
