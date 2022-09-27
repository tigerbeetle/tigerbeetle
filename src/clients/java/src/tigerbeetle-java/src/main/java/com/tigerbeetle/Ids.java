package com.tigerbeetle;

import java.nio.ByteBuffer;

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

    @Override
    public void add() {
        super.add();
    }

    public void add(final byte[] id) {
        super.add();
        setId(id);
    }

    public void add(final long leastSignificant, final long mostSignificant) {
        super.add();
        setId(leastSignificant, mostSignificant);
    }

    public byte[] getId() {
        return getUInt128(at(0));
    }

    public long getId(final UInt128 part) {
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
