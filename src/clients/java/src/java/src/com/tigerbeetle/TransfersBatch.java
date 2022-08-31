package com.tigerbeetle;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.UUID;

public final class TransfersBatch extends Batch {
    private static final class Struct {
        public static final int SIZE = 128;
        public static final byte[] RESERVED = new byte[16];
    }

    private int lenght;
    private final int capacity;

    public TransfersBatch(int capacity) {
        super(capacity * Struct.SIZE);

        this.lenght = 0;
        this.capacity = capacity;
    }

    public TransfersBatch(Transfer[] transfers) {
        super(transfers.length * Struct.SIZE);

        this.lenght = transfers.length;
        this.capacity = transfers.length;

        for (int i = 0; i < transfers.length; i++) {
            Set(i, transfers[i]);
        }
    }

    TransfersBatch(ByteBuffer buffer) {
        super(buffer);

        this.capacity = buffer.capacity() / Struct.SIZE;
        this.lenght = capacity;
    }

    public void Add(Transfer transfer) throws IndexOutOfBoundsException {
        Set(lenght, transfer);
    }

    public Transfer Get(int index) throws IndexOutOfBoundsException, BufferUnderflowException {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        Transfer transfer = new Transfer();

        ByteBuffer ptr = buffer.position(index * Struct.SIZE);
        transfer.setId(new UUID(ptr.getLong(), ptr.getLong()));
        transfer.setDebitAccountId(new UUID(ptr.getLong(), ptr.getLong()));
        transfer.setCreditAccountId(new UUID(ptr.getLong(), ptr.getLong()));
        transfer.setUserData(new UUID(ptr.getLong(), ptr.getLong()));
        ptr = ptr.position(ptr.position() + Struct.RESERVED.length);
        transfer.setPendingId(new UUID(ptr.getLong(), ptr.getLong()));
        transfer.setTimeout(ptr.getLong());
        transfer.setLedger(ptr.getInt());
        transfer.setCode(ptr.getShort());
        transfer.setFlags(ptr.getShort());
        transfer.setAmount(ptr.getLong());
        transfer.setTimeout(ptr.getLong());

        return transfer;

    }

    public void Set(int index, Transfer transfer) throws IndexOutOfBoundsException, NullPointerException {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();
        if (transfer == null)
            throw new NullPointerException();

        final int start = index * Struct.SIZE;
        ByteBuffer ptr = buffer.position(start);

        ptr
                .putLong(transfer.getId().getMostSignificantBits())
                .putLong(transfer.getId().getLeastSignificantBits())
                .putLong(transfer.getDebitAccountId().getMostSignificantBits())
                .putLong(transfer.getDebitAccountId().getLeastSignificantBits())
                .putLong(transfer.getCreditAccountId().getMostSignificantBits())
                .putLong(transfer.getCreditAccountId().getLeastSignificantBits())
                .putLong(transfer.getUserData().getMostSignificantBits())
                .putLong(transfer.getUserData().getLeastSignificantBits())
                .put(Struct.RESERVED)
                .putLong(transfer.getPendingId().getMostSignificantBits())
                .putLong(transfer.getPendingId().getLeastSignificantBits())
                .putLong(transfer.getTimeout())
                .putInt(transfer.getLedger())
                .putShort(transfer.getCode())
                .putShort(transfer.getFlags())
                .putLong(transfer.getAmount())
                .putLong(transfer.getTimeout());

        if (ptr.position() - start != Struct.SIZE)
            throw new IndexOutOfBoundsException("Unexpected account size");
        if (index >= lenght)
            lenght = index + 1;
    }

    public int getLenght() {
        return this.lenght;
    }

    public int getCapacity() {
        return this.capacity;
    }

    public Transfer[] toArray() throws BufferUnderflowException {
        Transfer[] array = new Transfer[lenght];
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
