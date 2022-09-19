package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UUIDsBatch extends Batch {

    interface Struct {
        public static final int SIZE = 16;
    }

    private int lenght;
    private final int capacity;

    public UUIDsBatch(int capacity) {
        super(capacity * Struct.SIZE);

        this.lenght = 0;
        this.capacity = capacity;
    }

    public UUIDsBatch(UUID[] uuids) {
        super(uuids.length * Struct.SIZE);

        this.lenght = uuids.length;
        this.capacity = uuids.length;

        for (int i = 0; i < uuids.length; i++) {
            set(i, uuids[i]);
        }
    }

    UUIDsBatch(ByteBuffer buffer) {
        super(buffer);

        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        if (bufferLen % Struct.SIZE != 0)
            throw new AssertionError(
                    "Invalid data received from completion handler. bufferLen=%d, sizeOf(UUID)=%d.",
                    bufferLen, Struct.SIZE);

        this.capacity = bufferLen / Struct.SIZE;
        this.lenght = capacity;
    }

    public void add(UUID uuid) {
        set(lenght, uuid);
    }

    public UUID get(int index) {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        var ptr = getBuffer().position(index * Struct.SIZE);
        return uuidFromBuffer(ptr);
    }

    public void set(int index, UUID uuid) {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        if (uuid == null)
            throw new NullPointerException();

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

    @Override
    public int getLenght() {
        return this.lenght;
    }

    public int getCapacity() {
        return this.capacity;
    }

    public UUID[] toArray() {
        var array = new UUID[lenght];
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
