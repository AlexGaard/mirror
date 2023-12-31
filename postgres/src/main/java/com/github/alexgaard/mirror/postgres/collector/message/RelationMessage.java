package com.github.alexgaard.mirror.postgres.collector.message;

import com.github.alexgaard.mirror.core.exception.ParseException;
import com.github.alexgaard.mirror.postgres.utils.PgoutputParser;

import java.util.ArrayList;
import java.util.List;

import static com.github.alexgaard.mirror.postgres.collector.message.MessageParser.badMessageId;

public class RelationMessage extends Message {

    public static final char ID = 'R';

    public final int oid;

    public final String namespace;

    public final String relationName;

    public final byte replicaId;


    public final List<Column> columns;

    protected RelationMessage(String lsn, int xid, int oid, String namespace, String relationName, byte replicaId, List<Column> columns) {
        super(lsn, xid, Type.RELATION);
        this.oid = oid;
        this.namespace = namespace;
        this.relationName = relationName;
        this.replicaId = replicaId;
        this.columns = columns;
    }

    /*
    Format:
        Byte1('R')
        Identifies the message as a relation message.

        Int32 (TransactionId)
        Xid of the transaction (only present for streamed transactions). This field is available since protocol version 2.

        Int32 (Oid)
        OID of the relation.

        String
        Namespace (empty string for pg_catalog).

        String
        Relation name.

        Int8
        Replica identity setting for the relation (same as relreplident in pg_class).

        Int16
        Number of columns.

        Next, the following message part appears for each column included in the publication (except generated columns):

        Int8
        Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key.

        String
        Name of the column.

        Int32 (Oid)
        OID of the column's data type.

        Int32
        Type modifier of the column (atttypmod).
    */

    public static RelationMessage parse(RawMessage msg) {
        PgoutputParser parser = new PgoutputParser(msg.data);

        char type = parser.nextChar();

        if (type != ID) {
            throw badMessageId(ID, type);
        }

        int oid = parser.nextInt();

        String namespace = parser.nextString();

        String relationName = parser.nextString();

        byte replicaId = parser.nextByte();

        short numCols = parser.nextShort();

        List<Column> columns = parseColumns(numCols, parser);

        return new RelationMessage(msg.lsn, msg.xid, oid, namespace, relationName, replicaId, columns);
    }

    private static List<Column> parseColumns(int numCols, PgoutputParser parser) {
        List<Column> columns = new ArrayList<>(numCols);

        for (short s = 0; s < numCols; s++) {
            boolean partOfKey = parser.nextByte() > 0;
            String columnName = parser.nextString();
            int dataOid = parser.nextInt();
            int typeMod = parser.nextInt();

            columns.add(new Column(partOfKey, columnName, dataOid, typeMod));
        }

        return columns;
    }

    public static class Column {

        public final boolean partOfKey;

        public final String name;

        public final int dataOid;

        public final int typeModifier;

        public Column(boolean partOfKey, String name, int dataOid, int typeModifier) {
            this.partOfKey = partOfKey;
            this.name = name;
            this.dataOid = dataOid;
            this.typeModifier = typeModifier;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationMessage that = (RelationMessage) o;

        if (oid != that.oid) return false;
        if (replicaId != that.replicaId) return false;
        if (!namespace.equals(that.namespace)) return false;
        if (!relationName.equals(that.relationName)) return false;
        return columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        int result = oid;
        result = 31 * result + namespace.hashCode();
        result = 31 * result + relationName.hashCode();
        result = 31 * result + (int) replicaId;
        result = 31 * result + columns.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RelationMessage{" +
                "oid=" + oid +
                ", namespace='" + namespace + '\'' +
                ", relationName='" + relationName + '\'' +
                ", replicaId=" + replicaId +
                ", columns=" + columns +
                ", lsn='" + lsn + '\'' +
                ", xid=" + xid +
                ", type=" + type +
                '}';
    }
}
