package com.github.alexgaard.mirror.postgres.collector.message;

public class CommitMessage extends Message {
    protected CommitMessage(String lsn, int xid) {
        super(lsn, xid, Type.COMMIT);
    }

    public static CommitMessage parse(RawMessage event) {
        return new CommitMessage(event.lsn, event.xid);
    }

}
