package com.tigerbeetle;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

class CreateTransfersResultBatch extends Batch {

    private final int lenght;

    public CreateTransfersResultBatch(ByteBuffer buffer)
            throws RequestException {
        super(buffer);

        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        if (bufferLen % CreateTransfersResult.Struct.SIZE != 0)
            throw new RequestException(RequestException.Status.INVALID_DATA_SIZE);

        this.lenght = bufferLen / CreateTransfersResult.Struct.SIZE;
    }

    public CreateTransfersResult get(int index)
            throws IndexOutOfBoundsException, BufferUnderflowException {
        if (index < 0 || index >= lenght)
            throw new IndexOutOfBoundsException();

        var ptr = buffer.position(index * CreateTransfersResult.Struct.SIZE);
        return new CreateTransfersResult(ptr);
    }

    public CreateTransfersResult[] toArray()
            throws BufferUnderflowException {
        var array = new CreateTransfersResult[lenght];
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
