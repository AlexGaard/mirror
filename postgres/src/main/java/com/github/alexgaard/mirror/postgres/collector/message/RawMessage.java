package com.github.alexgaard.mirror.postgres.collector.message;

import java.util.Arrays;

public class RawMessage {

    public final String lsn;

    public final int xid;

    public final byte[] data;

    public RawMessage(String lsn, int xid, byte[] data) {
        this.lsn = lsn;
        this.xid = xid;
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RawMessage that = (RawMessage) o;

        if (xid != that.xid) return false;
        if (!lsn.equals(that.lsn)) return false;
        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = lsn.hashCode();
        result = 31 * result + xid;
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "RawMessage{" +
                "lsn='" + lsn + '\'' +
                ", xid=" + xid +
                ", data=" + Arrays.toString(data) +
                '}';
    }
}
