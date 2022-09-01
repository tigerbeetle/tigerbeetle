package com.tigerbeetle;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

class CreateAccountsResultBatch extends Batch {

    private final int lenght;

    public CreateAccountsResultBatch(ByteBuffer buffer) {
        super(buffer);
        this.lenght = buffer.capacity() / CreateAccountsResult.Struct.SIZE;
    }

    public CreateAccountsResult get(int index) throws IndexOutOfBoundsException, BufferUnderflowException {
        if (index < 0 || index >= lenght)
            throw new IndexOutOfBoundsException();

        ByteBuffer ptr = buffer.position(index * CreateAccountsResult.Struct.SIZE);
        return new CreateAccountsResult(ptr);
    }

    public CreateAccountsResult[] toArray() throws BufferUnderflowException {
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
