package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A {@link Batch batch} of transfers.
 *
 * @see <a href="https://docs.tigerbeetle.com/reference/transfers">TigerBeetle Docs</a>.
 */
public final class TransferBatch extends Batch {

    interface Struct {

        int Id = 0;
        int DebitAccountId = 16;
        int CreditAccountId = 32;
        int UserData = 48;
        int Reserved = 64;
        int PendingId = 80;
        int Timeout = 96;
        int Ledger = 104;
        int Code = 108;
        int Flags = 110;
        int Amount = 112;
        int Timestamp = 120;

        int SIZE = 128;
    }

    static final TransferBatch EMPTY = new TransferBatch(0);

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
    public TransferBatch(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    TransferBatch(final ByteBuffer buffer) {
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#id">id</a>.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getId() {
        return getUInt128(at(Struct.Id));
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#id">id</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#id">id</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#id">id</a>.
     *
     * @param id an array of 16 bytes representing the 128-bit value.
     * @throws NullPointerException if {@code id} is null.
     * @throws IllegalArgumentException if {@code id} is not 16 bytes long.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setId(final byte[] id) {
        Objects.requireNonNull(id, "Id cannot be null");
        putUInt128(at(Struct.Id), id);
    }


    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#debit_account_id">debit
     * account id</a>.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getDebitAccountId() {
        return getUInt128(at(Struct.DebitAccountId));
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#debit_account_id">debit
     * account id</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#debit_account_id">debit
     * account id</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#debit_account_id">debit
     * account id</a>.
     *
     * @param debitAccountId an array of 16 bytes representing the 128-bit value.
     * @throws NullPointerException if {@code debitAccountId} is null.
     * @throws IllegalArgumentException if {@code debitAccountId} is not 16 bytes long.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setDebitAccountId(final byte[] debitAccountId) {
        Objects.requireNonNull(debitAccountId, "Debit account id cannot be null");
        putUInt128(at(Struct.DebitAccountId), debitAccountId);
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#credit_account_id">credit
     * account id</a>.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getCreditAccountId() {
        return getUInt128(at(Struct.CreditAccountId));
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#credit_account_id">credit
     * account id</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#credit_account_id">credit
     * account id</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#credit_account_id">credit
     * account id</a>.
     *
     * @param creditAccountId an array of 16 bytes representing the 128-bit value.
     * @throws NullPointerException if {@code creditAccountId} is null.
     * @throws IllegalArgumentException if {@code creditAccountId} is not 16 bytes long.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setCreditAccountId(final byte[] creditAccountId) {
        Objects.requireNonNull(creditAccountId, "Credit account id cannot be null");
        putUInt128(at(Struct.CreditAccountId), creditAccountId);
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#user_data">user_data</a>.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getUserData() {
        return getUInt128(at(Struct.UserData));
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#user_data">user_data</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#user_data">user_data</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#user_data">user_data</a>.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#pending_id">pending
     * id</a>.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getPendingId() {
        return getUInt128(at(Struct.PendingId));
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#pending_id">pending
     * id</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#pending_id">pending
     * id</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#pending_id">pending
     * id</a>.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#timeout">timeout</a>.
     *
     * @return a 64-bit integer.
     */
    public long getTimeout() {
        return getUInt64(at(Struct.Timeout));
    }

    /**
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#timeout">timeout</a>.
     *
     * @param timeout A 64-bit integer.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setTimeout(final long timeout) {
        putUInt64(at(Struct.Timeout), timeout);
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#ledger">ledger</a>.
     *
     * @return a 32-bit integer.
     */
    public int getLedger() {
        return getUInt32(at(Struct.Ledger));
    }

    /**
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#ledger">ledger</a>.
     *
     * @param ledger a 32-bit integer.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setLedger(final int ledger) {
        putUInt32(at(Struct.Ledger), ledger);
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#code">code</a>.
     *
     * @return a 16-bit unsigned integer.
     */
    public int getCode() {
        return getUInt16(at(Struct.Code));
    }

    /**
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#code">code</a>.
     *
     * @param code a 16-bit unsigned integer defined by the user.
     * @throws IllegalArgumentException if code is negative or greater than 65535.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setCode(final int code) {
        putUInt16(at(Struct.Code), code);
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#flags">flags</a>.
     *
     * @see com.tigerbeetle.TransferFlags
     * @return a 16-bit unsigned integer bit mask.
     */
    public int getFlags() {
        return getUInt16(at(Struct.Flags));
    }

    /**
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#flags">flags</a>.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#amount">amount</a>.
     * <p>
     * Must always be interpreted as an unsigned integer.
     *
     * @return A 64-bit integer.
     */
    public long getAmount() {
        return getUInt64(at(Struct.Amount));
    }

    /**
     * Sets the <a href="https://docs.tigerbeetle.com/reference/transfers#amount">amount</a>.
     * <p>
     * Must always be interpreted as an unsigned integer.
     *
     * @param amount a 64-bit integer.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setAmount(final long amount) {
        putUInt64(at(Struct.Amount), amount);
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/transfers#timestamp">timestamp</a>.
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
