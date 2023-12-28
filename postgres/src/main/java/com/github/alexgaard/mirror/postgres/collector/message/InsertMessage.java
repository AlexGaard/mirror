package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.core.exception.ParseException;
import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;

import java.util.List;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class InsertMessage extends Message {

    public static final char ID = 'I';

    public final int relationMessageOid;

    public final List<TupleDataColumn> columns;

    protected InsertMessage(String lsn, int xid, int relationMessageOid, List<TupleDataColumn> columns) {
        super(lsn, xid, Type.INSERT);
        this.relationMessageOid = relationMessageOid;
        this.columns = columns;
    }

    /*
    Format:
        Byte1('I')
        Identifies the message as an insert message.

        Int32 (TransactionId)
        Xid of the transaction (only present for streamed transactions). This field is available since protocol version 2.

        Int32 (Oid)
        OID of the relation corresponding to the ID in the relation message.

        Byte1('N')
        Identifies the following TupleData message as a new tuple.

        TupleData
        TupleData message part representing the contents of new tuple.
    */

    public static InsertMessage parse(RawMessage msg) {
        PgoutputParser parser = new PgoutputParser(msg.data);

        char type = parser.nextChar();

        if (type != ID) {
            throw badMessageId(ID, type);
        }

        int oid = parser.nextInt();

        // skip byte
        parser.nextChar();

        List<TupleDataColumn> columns = parser.nextTupleData();

        return new InsertMessage(msg.lsn, msg.xid, oid, columns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InsertMessage that = (InsertMessage) o;

        if (relationMessageOid != that.relationMessageOid) return false;
        return columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        int result = relationMessageOid;
        result = 31 * result + columns.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "InsertMessage{" +
                "relationMessageOid=" + relationMessageOid +
                ", columns=" + columns +
                ", lsn='" + lsn + '\'' +
                ", xid=" + xid +
                ", type=" + type +
                '}';
    }
}
