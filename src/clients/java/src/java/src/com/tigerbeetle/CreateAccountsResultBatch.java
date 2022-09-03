package com.tigerbeetle;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

class CreateAccountsResultBatch extends Batch {

    private final int lenght;

    public CreateAccountsResultBatch(ByteBuffer buffer)
            throws RequestException {
        super(buffer);

        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        if (bufferLen % CreateAccountsResult.Struct.SIZE != 0)
            throw new RequestException(RequestException.Status.INVALID_DATA_SIZE);

        this.lenght = bufferLen / CreateAccountsResult.Struct.SIZE;
    }

    public CreateAccountsResult get(int index)
            throws IndexOutOfBoundsException, BufferUnderflowException {
        if (index < 0 || index >= lenght)
            throw new IndexOutOfBoundsException();

        ByteBuffer ptr = buffer.position(index * CreateAccountsResult.Struct.SIZE);
        return new CreateAccountsResult(ptr);
    }

    public CreateAccountsResult[] toArray()
            throws BufferUnderflowException {
        CreateAccountsResult[] array = new CreateAccountsResult[lenght];
        for (int i = 0; i < lenght; i++) {
            array[i] = get(i);
        }
        return array;
    }

    @Override
    public long getBufferLen() {
        return lenght * CreateAccountsResult.Struct.SIZE;
    }
}
