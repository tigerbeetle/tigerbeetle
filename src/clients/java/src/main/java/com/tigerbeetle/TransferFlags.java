package com.tigerbeetle;

public interface TransferFlags {
    short NONE = 0;
    short LINKED = (short) (1 << 0);
    short PENDING = (short) (1 << 1);
    short POST_PENDING_TRANSFER = (short) (1 << 2);
    short VOID_PENDING_TRANSFER = (short) (1 << 3);
}
