package com.tigerbeetle;

import java.nio.ByteBuffer;
import com.tigerbeetle.UInt128.Bytes;

public final class Ids extends Batch {

    interface Struct {
        public static final int SIZE = 16;
    }

    static final Ids EMPTY = new Ids(0);

    public Ids(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    Ids(final ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }

    public byte[] getId() {
        return getUInt128(at(0));
    }

    public long getId(final Bytes part) {
        return getUInt128(at(0), part);
    }

    public void setId(final byte[] id) {
        if (id == null)
            throw new NullPointerException("Id cannot be null");

        putUInt128(at(0), id);
    }

    public void setId(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(0), leastSignificant, mostSignificant);
    }
}
