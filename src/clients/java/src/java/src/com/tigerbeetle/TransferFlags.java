package com.tigerbeetle;

public final class TransferFlags {

    public final static short NONE = 0;
    public final static short LINKED = (short) (1 << 0);
    public final static short PENDING = (short) (1 << 1);
    public final static short POST_PENDING_TRANSFER = (short) (1 << 2);
    public final static short VOID_PENDING_TRANSFER = (short) (1 << 3);

    private short value;

    public TransferFlags(short value) {
        this.value = value;
    }

    public TransferFlags() {
        this.value = NONE;
    }

    public TransferFlags setLinked(boolean linked) {

        final short flag = (short) (1 << 0);

        if (linked)
            value += flag;
        else
            value -= flag;

        return this;
    }

    public TransferFlags setPending(boolean pending) {
        final short flag = (short) (1 << 1);

        if (pending)
            value += flag;
        else
            value -= flag;

        return this;
    }

    public TransferFlags setPostPendingTransfer(boolean postPendingTransfer) {
        final short flag = (short) (1 << 2);

        if (postPendingTransfer)
            value += flag;
        else
            value -= flag;

        return this;
    }

    public TransferFlags setVoidPendingTransfer(boolean voidPendingTransfer) {
        final short flag = (short) (1 << 3);

        if (voidPendingTransfer)
            value += flag;
        else
            value -= flag;

        return this;
    }

    public short getValue() {
        return value;
    }
}
