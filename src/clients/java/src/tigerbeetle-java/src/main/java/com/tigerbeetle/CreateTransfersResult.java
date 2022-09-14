package com.tigerbeetle;

import java.nio.ByteBuffer;

public final class CreateTransfersResult {

    static final class Struct {
        public static final int SIZE = 8;
    }

    public final int index;
    public final CreateTransferResult result;

    CreateTransfersResult(ByteBuffer ptr) {
        index = ptr.getInt();
        result = CreateTransferResult.fromValue(ptr.getInt());
    }

    CreateTransfersResult(int index, CreateTransferResult result) {
        this.index = index;
        this.result = result;
    }
}
