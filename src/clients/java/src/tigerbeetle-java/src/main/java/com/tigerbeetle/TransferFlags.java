package com.tigerbeetle;

public interface TransferFlags {
    public final static short NONE = 0;
    public final static short LINKED = (short) (1 << 0);
    public final static short PENDING = (short) (1 << 1);
    public final static short POST_PENDING_TRANSFER = (short) (1 << 2);
    public final static short VOID_PENDING_TRANSFER = (short) (1 << 3);
}
