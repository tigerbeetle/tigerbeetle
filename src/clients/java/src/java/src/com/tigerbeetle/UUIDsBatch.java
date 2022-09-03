package com.tigerbeetle;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class UUIDsBatch extends Batch {

    private static final class Struct {
        public static final int SIZE = 16;
    }

    private int lenght;
    private final int capacity;

    public UUIDsBatch(int capacity) {
        super(capacity * Struct.SIZE);

        this.lenght = 0;
        this.capacity = capacity;
    }

    public UUIDsBatch(UUID[] uuids) {
        super(uuids.length * Struct.SIZE);

        this.lenght = uuids.length;
        this.capacity = uuids.length;

        for (int i = 0; i < uuids.length; i++) {
            Set(i, uuids[i]);
        }
    }

    UUIDsBatch(ByteBuffer buffer)
            throws RequestException {
        super(buffer);

        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        if (bufferLen % Struct.SIZE != 0)
            throw new RequestException(RequestException.Status.INVALID_DATA_SIZE);

        this.capacity = bufferLen / Struct.SIZE;
        this.lenght = capacity;
    }

    public void Add(UUID uuid)
            throws IndexOutOfBoundsException {
        Set(lenght, uuid);
    }

    public UUID Get(int index)
            throws IndexOutOfBoundsException, BufferUnderflowException {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();

        var ptr = buffer.position(index * Struct.SIZE);
        return new UUID(ptr.getLong(), ptr.getLong());
    }

    public void Set(int index, UUID uuid)
            throws IndexOutOfBoundsException, NullPointerException {
        if (index < 0 || index >= capacity)
            throw new IndexOutOfBoundsException();
        if (uuid == null)
            throw new NullPointerException();

        final int start = index * Struct.SIZE;
        var ptr = buffer.position(start);

        ptr
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits());

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

    public UUID[] toArray()
            throws BufferUnderflowException {
        var array = new UUID[lenght];
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
