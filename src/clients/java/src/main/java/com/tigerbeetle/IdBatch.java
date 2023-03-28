package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A {@link Batch batch} of 128-bit unsigned integers.
 */
public final class IdBatch extends Batch {

    interface Struct {
        int SIZE = 16;
    }

    static final IdBatch EMPTY = new IdBatch(0);

    /**
     * Constructs an empty batch of ids with the desired maximum capacity.
     * <p>
     * Once created, an instance cannot be resized, however it may contain any number of ids between
     * zero and its {@link #getCapacity capacity}.
     *
     * @param capacity the maximum capacity.
     *
     * @throws IllegalArgumentException if capacity is negative.
     */
    public IdBatch(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    IdBatch(final ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }

    /**
     * Adds a new id at the end of this batch.
     * <p>
     * If successful, moves the current {@link #setPosition position} to the newly created id.
     *
     * @param id an array of 16 bytes representing the 128-bit value.
     *
     * @throws IllegalStateException if this batch is read-only.
     * @throws IndexOutOfBoundsException if exceeds the batch's capacity.
     */
    public void add(final byte[] id) {
        super.add();
        setId(id);
    }

    /**
     * Adds a new id at the end of this batch.
     * <p>
     * If successful, moves the current {@link #setPosition position} to the newly created id.
     *
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
     * @param mostSignificant a {@code long} representing the the last 8 bytes of the 128-bit value.
     *
     * @throws IllegalStateException if this batch is read-only.
     * @throws IndexOutOfBoundsException if exceeds the batch's capacity.
     */
    public void add(final long leastSignificant, final long mostSignificant) {
        super.add();
        setId(leastSignificant, mostSignificant);
    }

    /**
     * Adds a new id at the end of this batch.
     * <p>
     * If successful, moves the current {@link #setPosition position} to the newly created id.
     *
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
     *
     * @throws IllegalStateException if this batch is read-only.
     * @throws IndexOutOfBoundsException if exceeds the batch's capacity.
     */
    public void add(final long leastSignificant) {
        super.add();
        setId(leastSignificant, 0);
    }

    /**
     * Gets the id.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getId() {
        return getUInt128(at(0));
    }

    /**
     * Gets the id.
     *
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be
     *        retrieved.
     * @return a {@code long} representing the the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getId(final UInt128 part) {
        return getUInt128(at(0), part);
    }

    /**
     * Sets the id.
     *
     * @param id an array of 16 bytes representing the 128-bit value.
     * @throws NullPointerException if {@code id} is null.
     * @throws IllegalArgumentException if {@code id} is not 16 bytes long.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setId(final byte[] id) {
        Objects.requireNonNull(id, "Id cannot be null");
        putUInt128(at(0), id);
    }

    /**
     * Sets the id.
     *
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
     * @param mostSignificant a {@code long} representing the the last 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setId(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(0), leastSignificant, mostSignificant);
    }
}
