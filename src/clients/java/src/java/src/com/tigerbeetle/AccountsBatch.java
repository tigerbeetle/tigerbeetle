package com.tigerbeetle;

import java.nio.ByteBuffer;

public final class AccountsBatch extends Batch {

    private int lenght;
    private final int capacity;

    public AccountsBatch(int capacity) {
        super(capacity * Account.Struct.SIZE);

        this.lenght = 0;
        this.capacity = capacity;
    }

    public AccountsBatch(Account[] accounts) {
        super(accounts.length * Account.Struct.SIZE);

        this.lenght = accounts.length;
        this.capacity = accounts.length;

        for (int i = 0; i < accounts.length; i++) {
            set(i, accounts[i]);
        }
    }

    AccountsBatch(ByteBuffer buffer)
            throws RequestException {

        super(buffer);

        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        if (bufferLen % Account.Struct.SIZE != 0)
            throw new AssertionError(
                    "Invalid data received from completion handler: bufferLen=%d, sizeOf(Account)=%d.",
                    bufferLen,
                    Account.Struct.SIZE);

        this.capacity = bufferLen / Account.Struct.SIZE;
        this.lenght = capacity;
    }

    public void add(Account account) {
        set(lenght, account);
    }

    public Account get(int index) {

        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        var ptr = getBuffer().position(index * Account.Struct.SIZE);
        return new Account(ptr);
    }

    public void set(int index, Account account) {

        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        if (account == null)
            throw new NullPointerException();

        final int start = index * Account.Struct.SIZE;
        var ptr = getBuffer().position(start);
        account.save(ptr);

        if (ptr.position() - start != Account.Struct.SIZE)
            throw new AssertionError("Unexpected position: ptr.position()=%d, start=%d, sizeOf(Account)=%d.",
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

    public Account[] toArray() {
        Account[] array = new Account[lenght];
        for (int i = 0; i < lenght; i++) {
            array[i] = get(i);
        }

        return array;
    }

    @Override
    public long getBufferLen() {
        return lenght * Account.Struct.SIZE;
    }
}
