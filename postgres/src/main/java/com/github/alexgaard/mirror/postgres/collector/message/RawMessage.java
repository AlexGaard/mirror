package com.github.alexgaard.mirror.postgres.collector.message;

public class RawMessage {

    public final String lsn;

    public final int xid;

    public final byte[] data;

    public RawMessage(String lsn, int xid, byte[] data) {
        this.lsn = lsn;
        this.xid = xid;
        this.data = data;
    }

}
