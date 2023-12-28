package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class TypeMessage extends Message {

    public static final char ID = 'Y';

    public final int oid;

    public final String namespace;

    public final String name;

    protected TypeMessage(String lsn, int xid, int oid, String namespace, String name) {
        super(lsn, xid, Type.TYPE);
        this.oid = oid;
        this.namespace = namespace;
        this.name = name;
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

        int oid = parser.nextInt();

        String namespace = parser.nextString();

        String name = parser.nextString();

        return new TypeMessage(msg.lsn, msg.xid, oid, namespace, name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TypeMessage that = (TypeMessage) o;

        if (oid != that.oid) return false;
        if (!namespace.equals(that.namespace)) return false;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = oid;
        result = 31 * result + namespace.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TypeMessage{" +
                "oid=" + oid +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", lsn='" + lsn + '\'' +
                ", xid=" + xid +
                ", type=" + type +
                '}';
    }
}
