package com.tigerbeetle;

import java.nio.ByteBuffer;

public class CreateTransferResults extends Batch {

    interface Struct {

        public static final int Index = 0;
        public static final int Result = 4;

        public final static int SIZE = 8;
    }

    static final CreateTransferResults EMPTY = new CreateTransferResults(0);

    CreateTransferResults(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    CreateTransferResults(ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }

    public int getIndex() {
        return getUInt32(at(Struct.Index));
    }

    public CreateTransferResult getResult() {
        final var value = getUInt32(at(Struct.Result));
        return CreateTransferResult.fromValue(value);
    }
}
