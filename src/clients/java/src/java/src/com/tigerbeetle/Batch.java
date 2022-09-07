package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class Batch {

    private final ByteBuffer buffer;

    protected Batch(int bufferCapacity) {
        this(ByteBuffer.allocateDirect(bufferCapacity));
    }

    protected Batch(ByteBuffer buffer) {

        if (buffer == null) throw new IllegalArgumentException("buffer");

        // We require little-endian architectures everywhere for efficient network
        // deserialization:
        this.buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public abstract long getBufferLen();

    public abstract int getLenght();
}
