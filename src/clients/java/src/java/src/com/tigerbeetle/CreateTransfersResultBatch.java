package com.tigerbeetle;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

class CreateTransfersResultBatch extends Batch {

    private static final class Struct {
        public static final int SIZE = 8;
    }

    private final int lenght;

    public CreateTransfersResultBatch(ByteBuffer buffer) {
        super(buffer);
        this.lenght = buffer.capacity() / Struct.SIZE;
    }

    public CreateTransfersResult Get(int index) throws IndexOutOfBoundsException, BufferUnderflowException {
        if (index < 0 || index >= lenght)
            throw new IndexOutOfBoundsException();

        ByteBuffer ptr = buffer.position(index * Struct.SIZE);
        var resultIndex = ptr.getInt();
        var resultValue = ptr.getInt();

        return new CreateTransfersResult(
                resultIndex,
                CreateTransferResult.fromValue(resultValue));
    }

    public CreateTransfersResult[] toArray() throws BufferUnderflowException {
        CreateTransfersResult[] array = new CreateTransfersResult[lenght];
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
