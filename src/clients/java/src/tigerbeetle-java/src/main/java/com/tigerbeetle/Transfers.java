package com.tigerbeetle;

import java.nio.ByteBuffer;

/**
 * A {@link Batch batch} of transfers.
 */
public final class Transfers extends Batch {

    interface Struct {

        public static final int Id = 0;
        public static final int DebitAccountId = 16;
        public static final int CreditAccountId = 32;
        public static final int UserData = 48;
        public static final int Reserved = 64;
        public static final int PendingId = 80;
        public static final int Timeout = 96;
        public static final int Ledger = 104;
        public static final int Code = 108;
        public static final int Flags = 110;
        public static final int Amount = 112;
        public static final int Timestamp = 120;

        public static final int SIZE = 128;
    }

    static final Transfers EMPTY = new Transfers(0);

    /**
     * Constructs an empty batch of transfers with the desired maximum capacity.
     * <p>
     * Once created, an instance cannot be resized, however it may contain any number of transfers
     * between zero and its {@link #getCapacity capacity}.
     *
     * @param capacity the maximum capacity.
     *
     * @throws IllegalArgumentException if capacity is negative.
     */
    public Transfers(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    Transfers(final ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }

    /**
     * Adds a new transfer at the end of this batch.
     * <p>
     * If successfully, moves the current {@link #setPosition position} to the newly created
     * transfer.
     *
     * @throws IllegalStateException if this batch is read-only.
     * @throws IndexOutOfBoundsException if exceeds the batch's capacity.
     */
    @Override
    public void add() {
        super.add();
    }

    /**
     * Gets a unique identifier for the transfer.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getId() {
        return getUInt128(at(Struct.Id));
    }

    /**
     * Gets a unique identifier for the transfer.
     *
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be
     *        retrieved.
     * @return a {@code long} representing the the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getId(final UInt128 part) {
        return getUInt128(at(Struct.Id), part);
    }

    /**
     * Sets a unique identifier for the transfer.
     *
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
     * @param mostSignificant a {@code long} representing the the last 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setId(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.Id), leastSignificant, mostSignificant);
    }

    /**
     * Sets a unique identifier for the transfer.
     *
     * @param id an array of 16 bytes representing the 128-bit value.
     * @throws NullPointerException if {@code id} is null.
     * @throws IllegalArgumentException if {@code id} is not 16 bytes long.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setId(final byte[] id) {

        if (id == null)
            throw new NullPointerException("Id cannot be null");

        putUInt128(at(Struct.Id), id);
    }


    /**
     * Gets the id that refers to the account to debit the transfer's {@link #setAmount amount}.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getDebitAccountId() {
        return getUInt128(at(Struct.DebitAccountId));
    }

    /**
     * Gets the id that refers to the account to debit the transfer's {@link #setAmount amount}.
     *
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be
     *        retrieved.
     * @return a {@code long} representing the the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getDebitAccountId(final UInt128 part) {
        return getUInt128(at(Struct.DebitAccountId), part);
    }

    /**
     * Sets the id that refers to the account to debit the transfer's {@link #setAmount amount}.
     *
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
     * @param mostSignificant a {@code long} representing the the last 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setDebitAccountId(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.DebitAccountId), leastSignificant, mostSignificant);
    }

    /**
     * Sets the id that refers to the account to debit the transfer's {@link #setAmount amount}.
     *
     * @param debitAccountId an array of 16 bytes representing the 128-bit value.
     * @throws NullPointerException if {@code debitAccountId} is null.
     * @throws IllegalArgumentException if {@code debitAccountId} is not 16 bytes long.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setDebitAccountId(final byte[] debitAccountId) {

        if (debitAccountId == null)
            throw new NullPointerException("Debit account id cannot be null");

        putUInt128(at(Struct.DebitAccountId), debitAccountId);
    }

    /**
     * Gets the id that refers to the account to credit the transfer's {@link #setAmount amount}.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getCreditAccountId() {
        return getUInt128(at(Struct.CreditAccountId));
    }

    /**
     * Gets the id that refers to the account to credit the transfer's {@link #setAmount amount}.
     *
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be
     *        retrieved.
     * @return a {@code long} representing the the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getCreditAccountId(final UInt128 part) {
        return getUInt128(at(Struct.CreditAccountId), part);
    }

    /**
     * Sets the id that refers to the account to credit the transfer's {@link #setAmount amount}.
     *
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
     * @param mostSignificant a {@code long} representing the the last 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setCreditAccountId(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.CreditAccountId), leastSignificant, mostSignificant);
    }

    /**
     * Sets the id that refers to the account to credit the transfer's {@link #setAmount amount}.
     *
     * @param creditAccountId an array of 16 bytes representing the 128-bit value.
     * @throws NullPointerException if {@code creditAccountId} is null.
     * @throws IllegalArgumentException if {@code creditAccountId} is not 16 bytes long.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setCreditAccountId(final byte[] creditAccountId) {

        if (creditAccountId == null)
            throw new NullPointerException("Credit account id cannot be null");

        putUInt128(at(Struct.CreditAccountId), creditAccountId);
    }

    /**
     * Gets an optional secondary identifier to link this transfer to an external entity.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getUserData() {
        return getUInt128(at(Struct.UserData));
    }

    /**
     * Gets an optional secondary identifier to link this transfer to an external entity.
     *
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be
     *        retrieved.
     * @return a {@code long} representing the the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getUserData(final UInt128 part) {
        return getUInt128(at(Struct.UserData), part);
    }

    /**
     * Sets an optional secondary identifier to link this transfer to an external entity.
     *
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
     * @param mostSignificant a {@code long} representing the the last 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setUserData(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.UserData), leastSignificant, mostSignificant);
    }

    /**
     * Sets an optional secondary identifier to link this transfer to an external entity.
     * <p>
     * May be zero, null values are converted to zero.
     *
     * @param userData an array of 16 bytes representing the 128-bit value.
     * @throws IllegalArgumentException if {@code userData} is not 16 bytes long.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setUserData(final byte[] userData) {
        putUInt128(at(Struct.UserData), userData);
    }


    /**
     * Gets the id that references the pending transfer.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getPendingId() {
        return getUInt128(at(Struct.PendingId));
    }

    /**
     * Gets the id that references the pending transfer.
     *
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be
     *        retrieved.
     * @return a {@code long} representing the the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getPendingId(final UInt128 part) {
        return getUInt128(at(Struct.PendingId), part);
    }

    /**
     * Sets the id that references the pending transfer.
     * <p>
     * Must be zero if this is not a {@link TransferFlags#POST_PENDING_TRANSFER post} or
     * {@link TransferFlags#VOID_PENDING_TRANSFER post} transfer.
     *
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
     * @param mostSignificant a {@code long} representing the the last 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setPendingId(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.PendingId), leastSignificant, mostSignificant);
    }

    /**
     * Sets an optional secondary identifier to link this account to an external entity.
     * <p>
     * Must be zero if this is not a {@link TransferFlags#POST_PENDING_TRANSFER post} or
     * {@link TransferFlags#VOID_PENDING_TRANSFER post} transfer. Null values are converted to zero.
     *
     * @param pendingId an array of 16 bytes representing the 128-bit value.
     * @throws IllegalArgumentException if {@code pendingId} is not 16 bytes long.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setPendingId(final byte[] pendingId) {
        putUInt128(at(Struct.PendingId), pendingId);
    }

    /**
     * Gets the the interval (in nanoseconds) after a {@link TransferFlags#PENDING pending}
     * transfer's creation that it may be posted or voided.
     *
     * @return a 64-bit integer.
     */
    public long getTimeout() {
        return getUInt64(at(Struct.Timeout));
    }

    /**
     * Sets the the interval (in nanoseconds) after a {@link TransferFlags#PENDING pending}
     * transfer's creation that it may be posted or voided.
     * <p>
     * Must be zero if this is not a {@link TransferFlags#PENDING pending} transfer.
     *
     * @param timeout A 64-bit integer.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setTimeout(final long timeout) {
        putUInt64(at(Struct.Timeout), timeout);
    }

    /**
     * Gets an identifier used to enforce that transfers must be between accounts of the same
     * {@link Accounts#setLedger ledger}.
     *
     * @return a 32-bit integer.
     */
    public int getLedger() {
        return getUInt32(at(Struct.Ledger));
    }

    /**
     * Sets an identifier used to enforce that transfers must be between accounts of the same
     * {@link Accounts#setLedger ledger}.
     *
     * @param ledger a 32-bit integer.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setLedger(final int ledger) {
        putUInt32(at(Struct.Ledger), ledger);
    }

    /**
     * Gets a user-defined enum denoting the reason for (or category of) the transfer.
     *
     * @return a 16-bit unsigned integer.
     */
    public int getCode() {
        return getUInt16(at(Struct.Code));
    }

    /**
     * Sets a user-defined enum denoting the reason for (or category of) the transfer.
     * <p>
     * Must be non-zero.
     *
     * @param code a 16-bit unsigned integer defined by the user.
     * @throws IllegalArgumentException if code is negative or greater than 65535.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setCode(final int code) {
        putUInt16(at(Struct.Code), code);
    }

    /**
     * Gets a bit field that specifies (optional) transfer behavior.
     *
     * @see com.tigerbeetle.TransferFlags
     * @return a 16-bit unsigned integer bit mask.
     */
    public int getFlags() {
        return getUInt16(at(Struct.Flags));
    }

    /**
     * Sets a bit field that specifies (optional) transfer behavior.
     *
     * @see com.tigerbeetle.TransferFlags
     * @param flags a 16-bit unsigned integer bit mask.
     * @throws IllegalArgumentException if flags is negative or greater than 65535.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setFlags(final int flags) {
        putUInt16(at(Struct.Flags), flags);
    }

    /**
     * Gets how much should be debited from the {@link #setDebitAccountId debit account} and
     * credited to the {@link #setCreditAccountId credit account}.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return A 64-bit integer.
     */
    public long getAmount() {
        return getUInt64(at(Struct.Amount));
    }

    /**
     * Sets how much should be debited from the {@link #setDebitAccountId debit account} and
     * credited to the {@link #setCreditAccountId credit account}.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @param amount a 64-bit integer.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setAmount(final long amount) {
        putUInt64(at(Struct.Amount), amount);
    }

    /**
     * Gets the time the transfer was created.
     * <p>
     * This is set by TigerBeetle. The format is UNIX timestamp in nanoseconds.
     *
     * @return a 64-bit integer.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getTimestamp() {
        return getUInt64(at(Struct.Timestamp));
    }

    void setTimestamp(final long timestamp) {
        putUInt64(at(Struct.Timestamp), timestamp);
    }
}
