package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UInt128Batch extends Batch {

    interface Struct {
        public static final int SIZE = 16;
    }

    private int lenght;
    private final int capacity;

    public UInt128Batch(int capacity) {
        super(capacity * Struct.SIZE);

        this.lenght = 0;
        this.capacity = capacity;
    }

    public UInt128Batch(UUID[] uuids) {
        super(uuids.length * Struct.SIZE);

        this.lenght = uuids.length;
        this.capacity = uuids.length;

        for (int i = 0; i < uuids.length; i++) {
            set(i, uuids[i]);
        }
    }

    public UInt128Batch(final byte[][] ids) {
        super(ids.length * Struct.SIZE);

        this.lenght = ids.length;
        this.capacity = ids.length;

        for (int i = 0; i < ids.length; i++) {
            set(i, ids[i]);
        }
    }

    UInt128Batch(ByteBuffer buffer) {
        super(buffer);

        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        if (bufferLen % Struct.SIZE != 0)
            throw new AssertionError(
                    "Invalid data received from completion handler. bufferLen=%d, sizeOf(UInt128)=%d.",
                    bufferLen, Struct.SIZE);

        this.capacity = bufferLen / Struct.SIZE;
        this.lenght = capacity;
    }

    public void add(UUID uuid) {
        set(lenght, uuid);
    }

    public void add(final byte[] id) {
        set(lenght, id);
    }

    public void add(final long leastSignificantBits, final long mostSignificantBits) {
        set(lenght, leastSignificantBits, mostSignificantBits);
    }

    public byte[] get(int index) {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        var array = new byte[Struct.SIZE];
        var ptr = getBuffer().position(index * Struct.SIZE);
        ptr.get(array);

        return array;
    }

    public void set(int index, UUID uuid) {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        if (uuid == null)
            throw new NullPointerException("Uuid cannot be null");

        final int start = index * Struct.SIZE;
        var ptr = getBuffer().position(start);

        ptr.putLong(uuid.getLeastSignificantBits()).putLong(uuid.getMostSignificantBits());

        if (ptr.position() - start != Struct.SIZE)
            throw new AssertionError(
                    "Unexpected position: ptr.position()=%d, start=%d, sizeOf(UUID)=%d.",
                    ptr.position(), start, Struct.SIZE);

        if (index >= lenght)
            lenght = index + 1;
    }

    public void set(int index, long leastSignificantBits, long mostSignificantBits) {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        final int start = index * Struct.SIZE;
        var ptr = getBuffer().position(start);

        ptr.putLong(leastSignificantBits);
        ptr.putLong(mostSignificantBits);

        if (ptr.position() - start != Struct.SIZE)
            throw new AssertionError(
                    "Unexpected position: ptr.position()=%d, start=%d, sizeOf(UInt128)=%d.",
                    ptr.position(), start, Struct.SIZE);

        if (index >= lenght)
            lenght = index + 1;
    }

    public void set(int index, byte[] id) {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        if (id == null)
            throw new NullPointerException("Id cannot be null");

        if (id.length != Struct.SIZE)
            throw new IllegalArgumentException("Id must be 16 bytes long");

        final int start = index * Struct.SIZE;
        var ptr = getBuffer().position(start);

        ptr.put(id);

        if (ptr.position() - start != Struct.SIZE)
            throw new AssertionError(
                    "Unexpected position: ptr.position()=%d, start=%d, sizeOf(UInt128)=%d.",
                    ptr.position(), start, Struct.SIZE);

        if (index >= lenght)
            lenght = index + 1;
    }

    @Override
    public int getLenght() {
        return this.lenght;
    }

    public int getCapacity() {
        return this.capacity;
    }

    public byte[][] toArray() {
        var array = new byte[lenght][];
        for (int i = 0; i < lenght; i++) {
            array[i] = get(i);
        }
        return array;
    }

    @Override
    public long getBufferLen() {
        return lenght * Struct.SIZE;
    }

}
