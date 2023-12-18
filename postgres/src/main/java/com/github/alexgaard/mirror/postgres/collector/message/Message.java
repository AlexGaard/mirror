package com.github.alexgaard.mirror.postgres.collector.message;

public abstract class Message {

    public enum Type {
        BEGIN,
        COMMIT,
        INSERT,
        RELATION,
        DELETE,
        UPDATE,

        MESSAGE,

        ORIGIN,

        TYPE,

        TRUNCATE
    }

    public final String lsn;

    public final int xid;

    public final Type type;

    protected Message(String lsn, int xid, Type type) {
        this.lsn = lsn;
        this.xid = xid;
        this.type = type;
    }

}
