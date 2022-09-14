package com.tigerbeetle;

import java.nio.ByteBuffer;

class CreateAccountsResultBatch extends Batch {

    private final int lenght;

    public CreateAccountsResultBatch(ByteBuffer buffer) throws RequestException {

        super(buffer);

        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        if (bufferLen % CreateAccountsResult.Struct.SIZE != 0)
            throw new AssertionError(
                    "Invalid data received from completion handler. bufferLen=%d, sizeOf(CreateAccountsResult)=%d.",
                    bufferLen, CreateAccountsResult.Struct.SIZE);

        this.lenght = bufferLen / CreateAccountsResult.Struct.SIZE;
    }

    public CreateAccountsResult get(int index) {
        if (index < 0 || index >= lenght)
            throw new IndexOutOfBoundsException();

        var ptr = getBuffer().position(index * CreateAccountsResult.Struct.SIZE);
        return new CreateAccountsResult(ptr);
    }

    public CreateAccountsResult[] toArray() {
        var array = new CreateAccountsResult[lenght];
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
        return lenght * CreateAccountsResult.Struct.SIZE;
    }
}
