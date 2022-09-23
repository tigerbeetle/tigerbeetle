package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

public abstract class Batch {

    // We require little-endian architectures everywhere for efficient network
    // deserialization:
    final static ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private final ByteBuffer buffer;

    protected Batch(int bufferCapacity) {

        if (bufferCapacity < 0)
            throw new IllegalArgumentException("Buffer capacity cannot be negative");

        this.buffer = ByteBuffer.allocateDirect(bufferCapacity).order(BYTE_ORDER);
    }

    protected Batch(ByteBuffer buffer) {

        if (buffer == null)
            throw new NullPointerException("Buffer cannot be null");

        this.buffer = buffer.order(BYTE_ORDER);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public abstract long getBufferLen();

    public abstract int getLenght();

    protected static UUID uuidFromBuffer(ByteBuffer buffer) {
        long leastSignificantBits = buffer.getLong();
        long mostSignificantBits = buffer.getLong();
        return new UUID(mostSignificantBits, leastSignificantBits);
    }
}
