package com.tigerbeetle;

import java.nio.ByteBuffer;

public final class TransfersBatch extends Batch {

    private int lenght;
    private final int capacity;

    public TransfersBatch(int capacity) {
        super(capacity * Transfer.Struct.SIZE);

        this.lenght = 0;
        this.capacity = capacity;
    }

    public TransfersBatch(Transfer[] transfers) {
        super(transfers.length * Transfer.Struct.SIZE);

        this.lenght = transfers.length;
        this.capacity = transfers.length;

        for (int i = 0; i < transfers.length; i++) {
            set(i, transfers[i]);
        }
    }

    TransfersBatch(ByteBuffer buffer)
            throws RequestException {

        super(buffer);

        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        if (bufferLen % Transfer.Struct.SIZE != 0)
            throw new AssertionError(
                    "Invalid data received from completion handler: bufferLen=%d, sizeOf(Transfer)=%d.",
                    bufferLen,
                    Transfer.Struct.SIZE);

        this.capacity = bufferLen / Transfer.Struct.SIZE;
        this.lenght = capacity;
    }

    public void add(Transfer transfer) {
        set(lenght, transfer);
    }

    public Transfer get(int index) {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        var ptr = getBuffer().position(index * Transfer.Struct.SIZE);
        return new Transfer(ptr);
    }

    public void set(int index, Transfer transfer) {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        if (transfer == null)
            throw new NullPointerException();

        final int start = index * Transfer.Struct.SIZE;
        var ptr = getBuffer().position(start);
        transfer.save(ptr);

        if (ptr.position() - start != Transfer.Struct.SIZE)
            throw new AssertionError("Unexpected position: ptr.position()=%d, start=%d, sizeOf(Transfer)=%d.",
                    ptr.position(),
                    start,
                    Account.Struct.SIZE);

        if (index >= lenght)
            lenght = index + 1;
    }

    @Override
    public int getLenght() {
        return this.lenght;
    }

    public int getCapacity() {
        return this.capacity;
    }

    public Transfer[] toArray() {
        var array = new Transfer[lenght];
        for (int i = 0; i < lenght; i++) {
            array[i] = get(i);
        }
        return array;
    }

    @Override
    public long getBufferLen() {
        return lenght * Transfer.Struct.SIZE;
    }
}
