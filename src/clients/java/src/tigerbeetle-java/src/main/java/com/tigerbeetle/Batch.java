package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import com.tigerbeetle.UInt128.Bytes;

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
     * Intended usage:
     *
     * <pre>
     *
     * // Populating a batch to send
     * for(var dto : myDataSource) {
     *
     *   requestBatch.add();
     *   requestBatch.setXyz(dto.Xyz);
     *
     * }
     *
     * var replyBatch = client.submit(requestBatch);
     *
     * // Reading from a reply
     * while(replyBatch.next()) {
     *
     *   var value = replyBatch.getXyz();
     *
     * }
     *
     * </pre>
     */
    // @formatter:off


    private enum CursorStatus {

        Begin,
        Valid,
        End;

        public static final int INVALID_POSITION = -1;
    }

    // We require little-endian architectures everywhere for efficient network
    // deserialization:
    final static ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private int position;
    private CursorStatus cursorStatus;
    private int length;

    private final int capacity;
    private final ByteBuffer buffer;

    private final int ELEMENT_SIZE;

    Batch(final int capacity, final int ELEMENT_SIZE) {

        if (capacity < 0)
            throw new IllegalArgumentException("Buffer capacity cannot be negative");

        if (ELEMENT_SIZE <= 0)
            throw new AssertionError("Element size cannot be zero or negative");

        this.ELEMENT_SIZE = ELEMENT_SIZE;

        this.length = 0;
        this.capacity = capacity;

        this.position = CursorStatus.INVALID_POSITION;
        this.cursorStatus = CursorStatus.End;

        final var bufferCapacity = capacity * ELEMENT_SIZE;
        this.buffer = ByteBuffer.allocateDirect(bufferCapacity).order(BYTE_ORDER);
    }

    Batch(final ByteBuffer buffer, final int ELEMENT_SIZE) {

        if (buffer == null)
            throw new NullPointerException("Buffer cannot be null");

        if (ELEMENT_SIZE <= 0)
            throw new AssertionError("Element size cannot be zero or negative");

        this.ELEMENT_SIZE = ELEMENT_SIZE;
        final var bufferLen = buffer.capacity();

        // Make sure the completion handler is giving us valid data
        if (bufferLen % ELEMENT_SIZE != 0)
            throw new AssertionError(
                    "Invalid data received from completion handler: bufferLen=%d, elementSize=%d.",
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
     * Adds a new element to this batch.
     * <p>
     * If successfully, moves the current {@link #setPosition position} to the newly created
     * element.
     *
     * @throws IllegalStateException if this batch is read-only.
     * @throws IndexOutOfBoundsException if exceeds the batch's capacity.
     */
    public void add() {

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
     */
    public final boolean next() {

        if (cursorStatus == CursorStatus.End)
            return false;

        final var nextPosition = position + 1;
        if (nextPosition >= this.length) {
            position = CursorStatus.INVALID_POSITION;
            cursorStatus = CursorStatus.End;
            return false;
        } else {
            setPosition(nextPosition);
            return true;
        }
    }

    /**
     * Returns true if the batch has more elements.
     *
     * @return true if and the batch has a more elements.
     */
    public final boolean isValidPosition() {
        return cursorStatus == CursorStatus.Valid;
    }

    /**
     * Resets the cursor
     */
    public final void reset() {
        position = CursorStatus.INVALID_POSITION;
        cursorStatus = length > 0 ? CursorStatus.Begin : CursorStatus.End;
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

    protected int at(final int fieldOffSet) {

        if (this.cursorStatus != CursorStatus.Valid)
            throw new IllegalStateException();

        return (this.position * ELEMENT_SIZE) + fieldOffSet;
    }

    protected final byte[] getUInt128(final int index) {
        byte[] bytes = new byte[16];
        buffer.position(index).get(bytes);
        return bytes;
    }

    protected final long getUInt128(final int index, final Bytes part) {
        switch (part) {
            case LeastSignificant:
                return buffer.getLong(index);
            case MostSignificant:
                return buffer.getLong(index + Long.BYTES);
            default:
                throw new IllegalArgumentException();
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
            throw new IllegalArgumentException("");
        buffer.putShort(index, (short) value);
    }
}
