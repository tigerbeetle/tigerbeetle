package com.tigerbeetle;

import java.nio.ByteBuffer;

class CreateTransfersResultBatch extends Batch {

    private final int lenght;

    public CreateTransfersResultBatch(ByteBuffer buffer) {
        super(buffer);

        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        if (bufferLen % CreateTransfersResult.Struct.SIZE != 0)
            throw new AssertionError(
                    "Invalid data received from completion handler. bufferLen=%d, sizeOf(CreateTransfersResult)=%d.",
                    bufferLen, CreateTransfersResult.Struct.SIZE);

        this.lenght = bufferLen / CreateTransfersResult.Struct.SIZE;
    }

    public CreateTransfersResult get(int index) {
        if (index < 0 || index >= lenght)
            throw new IndexOutOfBoundsException();

        var ptr = getBuffer().position(index * CreateTransfersResult.Struct.SIZE);
        return new CreateTransfersResult(ptr);
    }

    public CreateTransfersResult[] toArray() {
        var array = new CreateTransfersResult[lenght];
        for (int i = 0; i < lenght; i++) {
            array[i] = get(i);
        }
        return array;
    }

    @Override
    public int getLenght() {
        return lenght;
    }

    @Override
    public long getBufferLen() {
        return lenght * CreateTransfersResult.Struct.SIZE;
    }
}
