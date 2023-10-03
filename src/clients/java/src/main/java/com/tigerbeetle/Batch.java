package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import static com.tigerbeetle.AssertionError.assertTrue;

/**
 * A Batch is contiguous memory block representing a collection of elements of the same type with a
 * cursor pointing to its current position.
 * <p>
 * Initially the cursor is positioned before the first element and must be positioned by calling
 * {@link #next}, {@link #add}, or {@link #setPosition} prior to reading or writing an element.
 */
public abstract class Batch {

    // @formatter:off
    /*
     * Overview
     *
     * Batch uses a ByteArray to hold direct memory that both the Java side and the JNI side can access.
     *
     * We expose the API using the concept of "Cursor" familiar to JDBC's ResultSet, where a single Batch
     * instance points to multiple elements depending on the cursor position, eliminating individual instances
     * for each element.
     *
     */
    // @formatter:off

    private enum CursorStatus {

        Begin,
        Valid,
        End;

        public static final int INVALID_POSITION = -1;
    }

    final static ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

    static {
        // We require little-endian architectures everywhere for efficient network
        // deserialization:
        assertTrue(BYTE_ORDER == ByteOrder.LITTLE_ENDIAN, "Native byte order LITTLE ENDIAN expected");
    }

    private int position;
    private CursorStatus cursorStatus;
    private int length;

    private final int capacity;
    private final ByteBuffer buffer;

    private final int ELEMENT_SIZE;

    Batch(final int capacity, final int ELEMENT_SIZE) {

        assertTrue(ELEMENT_SIZE > 0, "Element size cannot be zero or negative");

        if (capacity < 0) throw new IllegalArgumentException("Buffer capacity cannot be negative");

        this.ELEMENT_SIZE = ELEMENT_SIZE;

        this.length = 0;
        this.capacity = capacity;

        this.position = CursorStatus.INVALID_POSITION;
        this.cursorStatus = CursorStatus.Begin;

        final var bufferCapacity = capacity * ELEMENT_SIZE;
        this.buffer = ByteBuffer.allocateDirect(bufferCapacity).order(BYTE_ORDER);
    }

    Batch(final ByteBuffer buffer, final int ELEMENT_SIZE) {

        assertTrue(ELEMENT_SIZE > 0, "Element size cannot be zero or negative");
        Objects.requireNonNull(buffer, "Buffer cannot be null");

        this.ELEMENT_SIZE = ELEMENT_SIZE;
        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        assertTrue(bufferLen % ELEMENT_SIZE == 0, "Invalid data received from completion handler: bufferLen=%d, elementSize=%d.",
                    bufferLen, ELEMENT_SIZE);

        this.capacity = bufferLen / ELEMENT_SIZE;
        this.length = capacity;

        this.position = CursorStatus.INVALID_POSITION;
        this.cursorStatus = CursorStatus.Begin;

        this.buffer = buffer.order(BYTE_ORDER);
    }

    /**
     * Tells whether or not this batch is read-only.
     *
     * @return true if this batch is read-only
     */
    public final boolean isReadOnly() {
        return buffer.isReadOnly();
    }

    /**
     * Adds a new element at the end of this batch.
     * <p>
     * If successful, moves the current {@link #setPosition position} to the newly created
     * element.
     *
     * @throws IllegalStateException if this batch is read-only.
     * @throws IndexOutOfBoundsException if exceeds the batch's capacity.
     */
    public final void add() {

        if (isReadOnly())
            throw new IllegalStateException("Cannot add an element in a read-only batch");

        final var currentLen = this.length;
        if (currentLen >= capacity)
            throw new IndexOutOfBoundsException(String.format(
                    "Cannot add an element because the batch's capacity of %d was exceeded",
                    capacity));

        this.length = currentLen + 1;
        setPosition(currentLen);
    }

    /**
     * Tries to move the current {@link #setPosition position} to the next element in this batch.
     *
     * @return true if moved or false if the end of the batch was reached.
     *
     * @throws IndexOutOfBoundsException if the batch is already at the end.
     */
    public final boolean next() {

        if (cursorStatus == CursorStatus.End)
            throw new IndexOutOfBoundsException("This batch reached the end");

        final var nextPosition = position + 1;
        if (nextPosition >= this.length) {
            position = CursorStatus.INVALID_POSITION;
            cursorStatus = this.length > 0 ? CursorStatus.End : CursorStatus.Begin;
            return false;
        } else {
            setPosition(nextPosition);
            return true;
        }
    }

    /**
     * Tells if the current position points to an valid element.
     *
     * @return false if the cursor is positioned before the first element or at the end.
     */
    public final boolean isValidPosition() {
        return cursorStatus == CursorStatus.Valid;
    }

    /**
     * Moves the cursor to the front of this Batch, before the first element.
     * <p>
     * This causes the batch to be iterable again by calling {@link #next()}.
     */
    public final void beforeFirst() {
        position = CursorStatus.INVALID_POSITION;
        cursorStatus = CursorStatus.Begin;
    }

    /**
     * Returns the current element's position.
     *
     * @return a zero-based index or {@code -1} if at the end of the batch.
     */
    public final int getPosition() {
        return this.position;
    }

    /**
     * Moves to the element int the specified position.
     *
     * @param newPosition a zero-based index.
     * @throws IndexOutOfBoundsException if {@code newPosition} is negative or greater than the
     *         batch's {@link #getLength length}.
     */
    public final void setPosition(final int newPosition) {
        if (newPosition < 0 || newPosition >= this.length)
            throw new IndexOutOfBoundsException();

        this.position = newPosition;
        this.cursorStatus = CursorStatus.Valid;
    }

    /**
     * Gets the number of elements in this batch
     */
    public final int getLength() {
        return length;
    }

    /**
     * Gets the maximum number of elements this batch can contain.
     */
    public final int getCapacity() {
        return capacity;
    }

    final ByteBuffer getBuffer() {
        return buffer.position(0);
    }

    final int getBufferLen() {
        return this.length * ELEMENT_SIZE;
    }

    protected final int at(final int fieldOffSet) {

        if (this.cursorStatus != CursorStatus.Valid)
            throw new IllegalStateException();

        final var elementPosition = this.position * ELEMENT_SIZE;
        return elementPosition + fieldOffSet;
    }

    protected final byte[] getUInt128(final int index) {
        byte[] bytes = new byte[16];
        buffer.position(index).get(bytes);
        return bytes;
    }

    protected final long getUInt128(final int index, final UInt128 part) {
        if (part == UInt128.LeastSignificant) {
            return buffer.getLong(index);
        } else {
            return buffer.getLong(index + Long.BYTES);
        }
    }

    protected final void putUInt128(final int index, final byte[] value) {

        if (value == null) {

            // By default we assume null as zero
            // The caller may throw a NullPointerException instead
            putUInt128(index, 0L, 0L);

        } else {

            if (value.length != 16)
                throw new IllegalArgumentException("UInt128 must be 16 bytes long");

            buffer.position(index).put(value);
        }
    }

    protected final void putUInt128(final int index, final long leastSignificant,
            final long mostSignificant) {
        buffer.putLong(index, leastSignificant);
        buffer.putLong(index + Long.BYTES, mostSignificant);
    }

    protected final long getUInt64(final int index) {
        return buffer.getLong(index);
    }

    protected final void putUInt64(final int index, final long value) {
        buffer.putLong(index, value);
    }

    protected final int getUInt32(final int index) {
        return buffer.getInt(index);
    }

    protected final void putUInt32(final int index, final int value) {
        buffer.putInt(index, value);
    }

    protected final int getUInt16(final int index) {
        return Short.toUnsignedInt(buffer.getShort(index));
    }

    protected final void putUInt16(final int index, final int value) {
        if (value < 0 || value > Character.MAX_VALUE)
            throw new IllegalArgumentException("Value must be a 16-bit unsigned integer");
        buffer.putShort(index, (short) value);
    }
}
