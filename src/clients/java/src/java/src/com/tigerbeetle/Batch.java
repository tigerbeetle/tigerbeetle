package com.tigerbeetle;

import java.nio.ByteBuffer;

public abstract class Batch {

    protected final static byte CREATE_ACCOUNTS = 3;
    protected final static byte CREATE_TRANSFERS = 4;
    protected final static byte LOOKUP_ACCOUNTS = 5;
    protected final static byte LOOKUP_TRANSFERS = 6;

    // Used by the JNI side
    @SuppressWarnings("unused")
    private final byte operation;
    
    // Used by the JNI side
    @SuppressWarnings("unused")
    private final int bufferCapacity;

    protected final ByteBuffer buffer;
    
    protected Batch(byte operation, int bufferCapacity)
    {
        this.operation = operation;
        this.bufferCapacity = bufferCapacity;
        this.buffer = ByteBuffer.allocateDirect(bufferCapacity);
    }
}
