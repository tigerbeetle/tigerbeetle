package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class Batch {
   
    protected final ByteBuffer buffer;
    
    protected Batch(int bufferCapacity)
    {
        this(ByteBuffer.allocateDirect(bufferCapacity));
    }

    protected Batch(ByteBuffer buffer)
    {
        // We require little-endian architectures everywhere for efficient network deserialization:
        this.buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    abstract long getBufferLen();
}
