package com.tigerbeetle;

import java.nio.ByteBuffer;

public class CreateAccountResults extends Batch {

    interface Struct {

        public static final int Index = 0;
        public static final int Result = 4;

        public static final int SIZE = 8;
    }

    static final CreateAccountResults EMPTY = new CreateAccountResults(0);

    CreateAccountResults(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    CreateAccountResults(ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }

    public int getIndex() {
        return getUInt32(at(Struct.Index));
    }

    public CreateAccountResult getResult() {
        final var value = getUInt32(at(Struct.Result));
        return CreateAccountResult.fromValue(value);
    }
}
