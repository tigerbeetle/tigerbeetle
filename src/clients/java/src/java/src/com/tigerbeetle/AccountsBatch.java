package com.tigerbeetle;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.UUID;

public final class AccountsBatch extends Batch
{
    private static final class Struct
    {
        public static final int SIZE = 128;
        public static final byte[] RESERVED = new byte[48];
    }

    private int lenght;
    private final int capacity;

    public AccountsBatch(int capacity)
    {
        super(capacity * Struct.SIZE);

        this.lenght = 0;
        this.capacity = capacity;
    }

    public AccountsBatch(Account[] accounts)
    {
        super(accounts.length * Struct.SIZE);

        this.lenght = accounts.length;
        this.capacity = accounts.length;
 
        for(int i=0; i<accounts.length;i++)
        {
            Set(i, accounts[i]);
        }
    }

    AccountsBatch(ByteBuffer buffer)
    {
        super(buffer);

        this.capacity = buffer.capacity() / Struct.SIZE;
        this.lenght = capacity;
    }

    public void Add(Account account) throws IndexOutOfBoundsException
    {
        Set(lenght, account);
    }

    public Account Get(int index) throws IndexOutOfBoundsException, BufferUnderflowException
    {
        if (index < 0 || index >= capacity) throw new IndexOutOfBoundsException();

        Account account = new Account();

        ByteBuffer ptr = buffer.position(index * Struct.SIZE);
        account.setId(new UUID(ptr.getLong(), ptr.getLong()));
        account.setUserData(new UUID(ptr.getLong(), ptr.getLong()));
        ptr = ptr.position(ptr.position() + Struct.RESERVED.length);
        account.setLedger(ptr.getInt());
        account.setCode(ptr.getShort());
        account.setFlags(AccountFlags.fromValue(ptr.getShort()));
        account.setDebitsPending(ptr.getLong());
        account.setDebitsPosted(ptr.getLong());
        account.setCreditsPending(ptr.getLong());
        account.setCreditsPosted(ptr.getLong());
        account.setTimestamp(ptr.getLong());

        return account;

    }

    public void Set(int index, Account account) throws IndexOutOfBoundsException, NullPointerException
    {
        if (index < 0 || index >= capacity) throw new IndexOutOfBoundsException();
        if (account == null) throw new NullPointerException();

        final int start = index * Struct.SIZE;
        ByteBuffer ptr = buffer.position(start);

        ptr
        .putLong(account.getId().getMostSignificantBits())
        .putLong(account.getId().getLeastSignificantBits())
        .putLong(account.getUserData().getMostSignificantBits())
        .putLong(account.getUserData().getLeastSignificantBits())
        .put(Struct.RESERVED)
        .putInt(account.getLedger())
        .putShort(account.getCode())
        .putShort(account.getFlags().value)
        .putLong(account.getDebitsPending())
        .putLong(account.getDebitsPosted())
        .putLong(account.getCreditsPending())
        .putLong(account.getCreditsPosted())
        .putLong(account.getTimestamp());

        if (ptr.position() - start != Struct.SIZE) throw new IndexOutOfBoundsException("Unexpected account size");
        if (index >= lenght) lenght = index + 1;
    }    

    public int getLenght()
    {
        return this.lenght;
    }

    public int getCapacity()
    {
        return this.capacity;
    }

    public Account[] toArray() throws BufferUnderflowException
    {
        Account[] array = new Account[lenght];
        for(int i=0; i<lenght;i++)
        {
            array[i] = Get(i);
        }
        return array;
    }

    @Override
    public long getBufferLen()
    {
        return lenght * Struct.SIZE;
    }
}
