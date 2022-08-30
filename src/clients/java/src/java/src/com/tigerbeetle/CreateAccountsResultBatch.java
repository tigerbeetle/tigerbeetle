package com.tigerbeetle;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

class CreateAccountsResultBatch extends Batch {

    private static final class Struct {
        public static final int SIZE = 8;
    }

    private final int lenght;

    public CreateAccountsResultBatch(ByteBuffer buffer) {
        super(buffer);
        this.lenght = buffer.capacity() / Struct.SIZE;
    }

    public CreateAccountsResult Get(int index) throws IndexOutOfBoundsException, BufferUnderflowException {
        if (index < 0 || index >= lenght)
            throw new IndexOutOfBoundsException();

        ByteBuffer ptr = buffer.position(index * Struct.SIZE);
        var resultIndex = ptr.getInt();
        var resultValue = ptr.getInt();

        return new CreateAccountsResult(
                resultIndex,
                CreateAccountResult.fromValue(resultValue));
    }

    public CreateAccountsResult[] toArray() throws BufferUnderflowException {
        CreateAccountsResult[] array = new CreateAccountsResult[lenght];
        for (int i = 0; i < lenght; i++) {
            array[i] = Get(i);
        }
        return array;
    }

    @Override
    public long getBufferLen() {
        return lenght * Struct.SIZE;
    }
}
