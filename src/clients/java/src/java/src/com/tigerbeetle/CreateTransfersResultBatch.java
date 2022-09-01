package com.tigerbeetle;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

class CreateTransfersResultBatch extends Batch {

    private final int lenght;

    public CreateTransfersResultBatch(ByteBuffer buffer) {
        super(buffer);
        this.lenght = buffer.capacity() / CreateTransfersResult.Struct.SIZE;
    }

    public CreateTransfersResult get(int index) throws IndexOutOfBoundsException, BufferUnderflowException {
        if (index < 0 || index >= lenght)
            throw new IndexOutOfBoundsException();

        ByteBuffer ptr = buffer.position(index * CreateTransfersResult.Struct.SIZE);
        return new CreateTransfersResult(ptr);
    }

    public CreateTransfersResult[] toArray() throws BufferUnderflowException {
        CreateTransfersResult[] array = new CreateTransfersResult[lenght];
        for (int i = 0; i < lenght; i++) {
            array[i] = get(i);
        }
        return array;
    }

    @Override
    public long getBufferLen() {
        return lenght * CreateTransfersResult.Struct.SIZE;
    }
}
