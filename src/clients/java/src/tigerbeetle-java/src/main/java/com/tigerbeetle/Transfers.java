package com.tigerbeetle;

import java.nio.ByteBuffer;
import com.tigerbeetle.UInt128.Bytes;

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


    public Transfers(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    Transfers(final ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }


    /**
     * <p>
     *
     * @return
     */
    public byte[] getId() {
        return getUInt128(at(Struct.Id));
    }

    /**
     * <p>
     *
     * @return
     */
    public long getId(final Bytes part) {
        return getUInt128(at(Struct.Id), part);
    }

    /**
     * <p>
     *
     * @param leastSignificant
     * @param mostSignificant
     * @throws NullPointerException if {@code id} is null.
     */
    public void setId(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.Id), leastSignificant, mostSignificant);
    }

    /**
     * <p>
     *
     * @param id
     * @throws NullPointerException if {@code id} is null.
     * @throws IllegalArgumentException if {@code id} is not 16 bytes long.
     */
    public void setId(final byte[] id) {

        if (id == null)
            throw new NullPointerException("Id cannot be null");

        putUInt128(at(Struct.Id), id);
    }


    /**
     * <p>
     *
     * @return
     */
    public byte[] getDebitAccountId() {
        return getUInt128(at(Struct.DebitAccountId));
    }

    /**
     * <p>
     *
     * @return
     */
    public long getDebitAccountId(final Bytes part) {
        return getUInt128(at(Struct.DebitAccountId), part);
    }

    /**
     * <p>
     *
     * @param leastSignificant
     * @param mostSignificant
     * @throws NullPointerException if {@code id} is null.
     */
    public void setDebitAccountId(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.DebitAccountId), leastSignificant, mostSignificant);
    }

    /**
     * <p>
     *
     * @param debitAccountId
     * @throws NullPointerException if {@code debitAccountId} is null.
     * @throws IllegalArgumentException if {@code debitAccountId} is not 16 bytes long.
     */
    public void setDebitAccountId(final byte[] debitAccountId) {

        if (debitAccountId == null)
            throw new NullPointerException("Debit account id cannot be null");

        putUInt128(at(Struct.DebitAccountId), debitAccountId);
    }

    /**
     * <p>
     *
     * @return
     */
    public byte[] getCreditAccountId() {
        return getUInt128(at(Struct.CreditAccountId));
    }

    /**
     * <p>
     *
     * @return
     */
    public long getCreditAccountId(final Bytes part) {
        return getUInt128(at(Struct.CreditAccountId), part);
    }

    /**
     * <p>
     *
     * @param leastSignificant
     * @param mostSignificant
     * @throws NullPointerException if {@code id} is null.
     */
    public void setCreditAccountId(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.CreditAccountId), leastSignificant, mostSignificant);
    }

    /**
     * <p>
     *
     * @param creditAccountId
     * @throws NullPointerException if {@code creditAccountId} is null.
     * @throws IllegalArgumentException if {@code creditAccountId} is not 16 bytes long.
     */
    public void setCreditAccountId(final byte[] creditAccountId) {

        if (creditAccountId == null)
            throw new NullPointerException("Credit account id cannot be null");

        putUInt128(at(Struct.CreditAccountId), creditAccountId);
    }

    /**
     * <p>
     *
     * @return
     */
    public byte[] getUserData() {
        return getUInt128(at(Struct.UserData));
    }

    /**
     * <p>
     *
     * @return
     */
    public long getUserData(final Bytes part) {
        return getUInt128(at(Struct.UserData), part);
    }

    /**
     * <p>
     *
     * @param leastSignificant
     * @param mostSignificant
     */
    public void setUserData(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.UserData), leastSignificant, mostSignificant);
    }

    /**
     * <p>
     * May be zero, null values are converted to zero.
     *
     * @param userData
     * @throws IllegalArgumentException if {@code userData} is not 16 bytes long.
     */
    public void setUserData(final byte[] userData) {
        putUInt128(at(Struct.UserData), userData);
    }



    /**
     * <p>
     *
     * @return
     */
    public byte[] getPendingId() {
        return getUInt128(at(Struct.PendingId));
    }

    /**
     * <p>
     *
     * @return
     */
    public long getPendingId(final Bytes part) {
        return getUInt128(at(Struct.PendingId), part);
    }

    /**
     * <p>
     *
     * @param leastSignificant
     * @param mostSignificant
     */
    public void setPendingId(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.PendingId), leastSignificant, mostSignificant);
    }

    /**
     * <p>
     * May be zero, null values are converted to zero.
     *
     * @param pendingId
     * @throws IllegalArgumentException if {@code pendingId} is not 16 bytes long.
     */
    public void setPendingId(final byte[] pendingId) {
        putUInt128(at(Struct.PendingId), pendingId);
    }

    /**
     *
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     */
    public long getTimeout() {
        return getUInt64(at(Struct.Timeout));
    }

    /**
     * @param timeout A 64-bit integer.
     */
    public void setTimeout(final long timeout) {
        putUInt64(at(Struct.Timeout), timeout);
    }

    /**
     * Gets an identifier used to enforce transfers between the same ledger.
     *
     * @return a 32-bit integer.
     */
    public int getLedger() {
        return getUInt32(at(Struct.Ledger));
    }

    /**
     * Sets an identifier used to enforce transfers between the same ledger.
     * <p>
     * Must be non-zero.
     * <p>
     * Example: 1 for USD and 2 for EUR.
     *
     * @param ledger a 32-bit integer defined by the user.
     */
    public void setLedger(final int ledger) {
        putUInt32(at(Struct.Ledger), ledger);
    }

    /**
     * Gets a reason for the transfer.
     *
     * @return a 16-bit unsigned integer.
     */
    public int getCode() {
        return getUInt16(at(Struct.Code));
    }

    /**
     * Sets a reason for the transfer.
     * <p>
     * Must be non-zero.
     * <p>
     * Example: 1 for deposit, 2 for settlement.
     *
     * @param code a 16-bit unsigned integer defined by the user.
     * @throws IllegalArgumentException if code is negative or greater than 65535.
     */
    public void setCode(final int code) {
        putUInt16(at(Struct.Code), code);
    }

    /**
     * Gets the behavior during transfers.
     *
     * @see com.tigerbeetle.TransferFlags
     * @return a 16-bit unsigned integer bit mask.
     */
    public int getFlags() {
        return getUInt16(at(Struct.Flags));
    }

    /**
     * Sets the behavior during transfers.
     * <p>
     *
     * @see com.tigerbeetle.TransferFlags
     * @param flags a 16-bit unsigned integer bit mask.
     * @throws IllegalArgumentException if flags is negative or greater than 65535.
     */
    public void setFlags(final int flags) {
        putUInt16(at(Struct.Flags), flags);
    }

    /**
     *
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return A 64-bit integer.
     */
    public long getAmount() {
        return getUInt64(at(Struct.Amount));
    }

    /**
     *
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @param amount a 64-bit integer.
     */
    public void setAmount(final long amount) {
        putUInt64(at(Struct.Amount), amount);
    }

    /**
     * Time transfer was created.
     * <p>
     * UNIX timestamp in nanoseconds.
     *
     * @return a 64-bit integer.
     */
    public long getTimestamp() {
        return getUInt64(at(Struct.Timestamp));
    }

    void setTimestamp(final long timestamp) {
        putUInt64(at(Struct.Timestamp), timestamp);
    }
}
