package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.core.exception.ParseException;
import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;

import java.util.List;

public class InsertMessage extends Message {

    public final int relationMessageOid;

    public final List<TupleDataColumn> columns;

    protected InsertMessage(String lsn, int xid, int relationMessageOid, List<TupleDataColumn> columns) {
        super(lsn, xid, Type.INSERT);
        this.relationMessageOid = relationMessageOid;
        this.columns = columns;
    }

//    Insert
//    Byte1('I')
//    Identifies the message as an insert message.
//
//    Int32 (TransactionId)
//    Xid of the transaction (only present for streamed transactions). This field is available since protocol version 2.
//
//    Int32 (Oid)
//    OID of the relation corresponding to the ID in the relation message.
//
//    Byte1('N')
//    Identifies the following TupleData message as a new tuple.
//
//    TupleData
//    TupleData message part representing the contents of new tuple.

//    Ex: 0x49 00 00 40 15 4E 00 01 74 00 00 00 02 34 32

    public static InsertMessage parse(RawMessage event) {
        PgoutputParser parser = new PgoutputParser(event.data);

        char type = parser.nextChar();

        if (type != 'I') {
            throw new ParseException("Bad type: " + type);
        }

        int oid = parser.nextInt();

        // skip byte
        parser.nextChar();

        List<TupleDataColumn> columns = parser.nextTupleData();

        return new InsertMessage(event.lsn, event.xid, oid, columns);
    }

}
