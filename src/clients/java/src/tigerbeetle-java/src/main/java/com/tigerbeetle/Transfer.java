package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class Transfer {

    interface Struct {
        public static final int SIZE = 128;
        public static final byte[] RESERVED = new byte[16];
    }

    private static final UUID ZERO = new UUID(0, 0);

    private UUID id;
    private UUID debitAccountId;
    private UUID creditAccountId;
    private UUID userData;
    private UUID pendingId;
    private long timeout;
    private int ledger;
    private short code;
    private short flags;
    private long amount = 0;
    private long timestamp;

    public Transfer() {
        id = ZERO;
        debitAccountId = ZERO;
        creditAccountId = ZERO;
        userData = ZERO;
        pendingId = ZERO;
        timeout = 0;
        ledger = 0;
        code = 0;
        flags = TransferFlags.NONE;
        amount = 0;
        timestamp = 0;
    }

    Transfer(ByteBuffer ptr) {

        id = Batch.uuidFromBuffer(ptr);
        debitAccountId = Batch.uuidFromBuffer(ptr);
        creditAccountId = Batch.uuidFromBuffer(ptr);
        userData = Batch.uuidFromBuffer(ptr);
        ptr = ptr.position(ptr.position() + Struct.RESERVED.length);
        pendingId = Batch.uuidFromBuffer(ptr);
        timeout = ptr.getLong();
        ledger = ptr.getInt();
        code = ptr.getShort();
        flags = ptr.getShort();
        amount = ptr.getLong();
        timestamp = ptr.getLong();
    }

    /**
     * Gets an identifier for this transfer, defined by the user.
     *
     * @return An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any other unique
     *         128-bit integer.
     */
    public UUID getId() {
        return id;
    }

    /**
     * Sets an identifier for this transfer, defined by the user.
     * <p>
     * Must be unique and non-zero.
     *
     * @param id An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any other
     *        unique 128-bit integer.
     * @throws NullPointerException if {@code id} is null.
     */
    public void setId(UUID id) {
        if (id == null)
            throw new NullPointerException();

        this.id = id;
    }

    /**
     *
     *
     * @return An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any other unique
     *         128-bit integer.
     */
    public UUID getDebitAccountId() {
        return debitAccountId;
    }

    /**
     *
     * <p>
     * Must be unique and non-zero.
     *
     * @param debitAccountId An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any
     *        other unique 128-bit integer.
     * @throws NullPointerException if {@code debitAccountId} is null.
     */
    public void setDebitAccountId(UUID debitAccountId) {
        if (debitAccountId == null)
            throw new NullPointerException();

        this.debitAccountId = debitAccountId;
    }

    /**
     *
     *
     * @return An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any other unique
     *         128-bit integer.
     */
    public UUID getCreditAccountId() {
        return creditAccountId;
    }

    /**
     *
     * <p>
     * Must be unique and non-zero.
     *
     * @param creditAccountId An {@link java.util.UUID} representing an integer-encoded UUIDv4 or
     *        any other unique 128-bit integer.
     * @throws NullPointerException if {@code creditAccountId} is null.
     */
    public void setCreditAccountId(UUID creditAccountId) {
        if (creditAccountId == null)
            throw new NullPointerException();

        this.creditAccountId = creditAccountId;
    }

    /**
     * Gets the secondary identifier to link this transfer to an external entity.
     *
     * @return An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any other unique
     *         128-bit integer.
     */
    public UUID getUserData() {
        return userData;
    }

    /**
     * Sets the secondary identifier to link this transfer to an external entity.
     * <p>
     * May be zero, null values are converted to zero.
     *
     * @param userData An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any other
     *        unique 128-bit integer.
     */
    public void setUserData(UUID userData) {
        if (userData == null) {
            this.userData = ZERO;
        } else {
            this.userData = userData;
        }
    }

    /**
     *
     *
     * @return An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any other unique
     *         128-bit integer.
     */
    public UUID getPendingId() {
        return pendingId;
    }

    /**
     *
     * <p>
     * May be zero, null values are converted to zero.
     *
     * @param pendingId An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any
     *        other unique 128-bit integer.
     */
    public void setPendingId(UUID pendingId) {
        if (pendingId == null) {
            this.pendingId = ZERO;
        } else {
            this.pendingId = pendingId;
        }
    }

    /**
     *
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return A 64-bits integer.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * @param timeout A 64-bits integer.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * Gets an identifier used to enforce transfers between the same ledger.
     *
     * @return A 32-bit integer.
     */
    public int getLedger() {
        return ledger;
    }

    /**
     * Sets an identifier used to enforce transfers between the same ledger.
     * <p>
     * Must be non-zero.
     * <p>
     * Example: 1 for USD and 2 for EUR.
     *
     * @param ledger A 32-bit integer defined by the user.
     */
    public void setLedger(int ledger) {
        this.ledger = ledger;
    }

    /**
     * Gets a reason for the transfer.
     *
     * @return A 16-bits unsigned integer.
     */
    public int getCode() {
        return code;
    }

    /**
     * Sets a reason for the transfer.
     * <p>
     * Must be non-zero.
     * <p>
     * Example: 1 for deposit, 2 for settlement.
     *
     * @param code A 16-bits unsigned integer defined by the user.
     * @throws IllegalArgumentException if code is negative or greater than 65535.
     */
    public void setCode(int code) {
        if (code < 0 || code > Character.MAX_VALUE)
            throw new IllegalArgumentException("Code must be a unsigned 16 bits value");

        this.code = (short) code;
    }

    /**
     * Gets the behavior during transfers.
     *
     * @see com.tigerbeetle.TransferFlags
     * @return A 16-bits unsigned integer bit mask.
     */
    public int getFlags() {
        return flags;
    }

    /**
     * Sets the behavior during transfers.
     * <p>
     *
     * @see com.tigerbeetle.TransferFlags
     * @param flags A 16-bits unsigned integer bit mask.
     * @throws IllegalArgumentException if flags is negative or greater than 65535.
     */
    public void setFlags(int flags) {
        if (flags < 0 || flags > Character.MAX_VALUE)
            throw new IllegalArgumentException("Flags must be a 16-bits unsigned integer");

        this.flags = (short) flags;
    }

    /**
     *
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return A 64-bits integer.
     */
    public long getAmount() {
        return amount;
    }

    /**
     *
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @param amount A 64-bits integer.
     */
    public void setAmount(long amount) {
        this.amount = amount;
    }

    /**
     * Time transfer was created.
     * <p>
     * UNIX timestamp in nanoseconds.
     *
     * @return A 64-bits integer.
     */
    public long getTimestamp() {
        return timestamp;
    }

    void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    void save(ByteBuffer ptr) {
        ptr.putLong(id.getLeastSignificantBits());
        ptr.putLong(id.getMostSignificantBits());
        ptr.putLong(debitAccountId.getLeastSignificantBits());
        ptr.putLong(debitAccountId.getMostSignificantBits());
        ptr.putLong(creditAccountId.getLeastSignificantBits());
        ptr.putLong(creditAccountId.getMostSignificantBits());
        ptr.putLong(userData.getLeastSignificantBits());
        ptr.putLong(userData.getMostSignificantBits());
        ptr.put(Struct.RESERVED);
        ptr.putLong(pendingId.getLeastSignificantBits());
        ptr.putLong(pendingId.getMostSignificantBits());
        ptr.putLong(timeout);
        ptr.putInt(ledger);
        ptr.putShort(code);
        ptr.putShort(flags);
        ptr.putLong(amount);
        ptr.putLong(timestamp);
    }

}
