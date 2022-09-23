package com.tigerbeetle;

import java.nio.ByteBuffer;
import com.tigerbeetle.UInt128.Bytes;

public final class Transfer {

    interface Struct {
        public static final int SIZE = 128;
        public static final byte[] RESERVED = new byte[16];
    }

    private long idLeastSignificant;
    private long idMostSignificant;
    private long debitAccountIdLeastSignificant;
    private long debitAccountIdMostSignificant;
    private long creditAccountIdLeastSignificant;
    private long creditAccountIdMostSignificant;
    private long userDataLeastSignificant;
    private long userDataMostSignificant;
    private long pendingIdLeastSignificant;
    private long pendingIdMostSignificant;
    private long timeout;
    private int ledger;
    private short code;
    private short flags;
    private long amount;
    private long timestamp;

    public Transfer() {
        idLeastSignificant = 0L;
        idMostSignificant = 0L;
        debitAccountIdLeastSignificant = 0L;
        debitAccountIdMostSignificant = 0L;
        creditAccountIdLeastSignificant = 0L;
        creditAccountIdMostSignificant = 0L;
        userDataLeastSignificant = 0L;
        userDataMostSignificant = 0L;
        pendingIdLeastSignificant = 0L;
        pendingIdMostSignificant = 0L;
        timeout = 0L;
        ledger = 0;
        code = 0;
        flags = TransferFlags.NONE;
        amount = 0L;
        timestamp = 0L;
    }

    Transfer(ByteBuffer ptr) {

        idLeastSignificant = ptr.getLong();
        idMostSignificant = ptr.getLong();
        debitAccountIdLeastSignificant = ptr.getLong();
        debitAccountIdMostSignificant = ptr.getLong();
        creditAccountIdLeastSignificant = ptr.getLong();
        creditAccountIdMostSignificant = ptr.getLong();
        userDataLeastSignificant = ptr.getLong();
        userDataMostSignificant = ptr.getLong();
        ptr = ptr.position(ptr.position() + Struct.RESERVED.length);
        pendingIdLeastSignificant = ptr.getLong();
        pendingIdMostSignificant = ptr.getLong();
        timeout = ptr.getLong();
        ledger = ptr.getInt();
        code = ptr.getShort();
        flags = ptr.getShort();
        amount = ptr.getLong();
        timestamp = ptr.getLong();
    }

    /**
     * <p>
     *
     * @return
     */
    public byte[] getId() {
        return UInt128.toBytes(idLeastSignificant, idMostSignificant);
    }

    /**
     * <p>
     *
     * @return
     */
    public long getId(Bytes part) {
        switch (part) {
            case LeastSignificant:
                return idLeastSignificant;
            case MostSignificant:
                return idMostSignificant;
            default:
                throw new IllegalArgumentException("Invalid byte part");
        }
    }

    /**
     * <p>
     *
     * @param leastSignificant
     * @param mostSignificant
     */
    public void setId(final long leastSignificant, final long mostSignificant) {
        this.idLeastSignificant = leastSignificant;
        this.idMostSignificant = mostSignificant;
    }

    /**
     * <p>
     *
     * @param id
     * @throws NullPointerException if {@code id} is null.
     * @throws IllegalArgumentException if {@code id} is not 16 bytes long.
     */
    public void setId(final byte[] id) {
        this.idLeastSignificant = UInt128.getLong(id, Bytes.LeastSignificant);
        this.idMostSignificant = UInt128.getLong(id, Bytes.MostSignificant);
    }

    /**
     * <p>
     *
     * @return
     */
    public byte[] getDebitAccountId() {
        return UInt128.toBytes(debitAccountIdLeastSignificant, debitAccountIdMostSignificant);
    }

    /**
     * <p>
     *
     * @return
     */
    public long getDebitAccountId(Bytes part) {
        switch (part) {
            case LeastSignificant:
                return debitAccountIdLeastSignificant;
            case MostSignificant:
                return debitAccountIdMostSignificant;
            default:
                throw new IllegalArgumentException("Invalid byte part");
        }
    }

    /**
     * <p>
     *
     * @param leastSignificant
     * @param mostSignificant
     */
    public void setDebitAccountId(final long leastSignificant, final long mostSignificant) {
        this.debitAccountIdLeastSignificant = leastSignificant;
        this.debitAccountIdMostSignificant = mostSignificant;
    }

    /**
     * <p>
     *
     * @param id
     * @throws NullPointerException if {@code id} is null.
     * @throws IllegalArgumentException if {@code id} is not 16 bytes long.
     */
    public void setDebitAccountId(final byte[] id) {
        this.debitAccountIdLeastSignificant = UInt128.getLong(id, Bytes.LeastSignificant);
        this.debitAccountIdMostSignificant = UInt128.getLong(id, Bytes.MostSignificant);
    }

    /**
     * <p>
     *
     * @return
     */
    public byte[] getCreditAccountId() {
        return UInt128.toBytes(creditAccountIdLeastSignificant, creditAccountIdMostSignificant);
    }

    /**
     * <p>
     *
     * @return
     */
    public long getCreditAccountId(Bytes part) {
        switch (part) {
            case LeastSignificant:
                return creditAccountIdLeastSignificant;
            case MostSignificant:
                return creditAccountIdMostSignificant;
            default:
                throw new IllegalArgumentException("Invalid byte part");
        }
    }

    /**
     * <p>
     *
     * @param leastSignificant
     * @param mostSignificant
     */
    public void setCreditAccountId(final long leastSignificant, final long mostSignificant) {
        this.creditAccountIdLeastSignificant = leastSignificant;
        this.creditAccountIdMostSignificant = mostSignificant;
    }

    /**
     * <p>
     *
     * @param id
     * @throws NullPointerException if {@code id} is null.
     * @throws IllegalArgumentException if {@code id} is not 16 bytes long.
     */
    public void setCreditAccountId(final byte[] id) {
        this.creditAccountIdLeastSignificant = UInt128.getLong(id, Bytes.LeastSignificant);
        this.creditAccountIdMostSignificant = UInt128.getLong(id, Bytes.MostSignificant);
    }

    /**
     * <p>
     *
     * @return
     */
    public byte[] getUserData() {
        return UInt128.toBytes(userDataLeastSignificant, userDataMostSignificant);
    }

    /**
     * <p>
     *
     * @return
     */
    public long getUserData(Bytes part) {
        switch (part) {
            case LeastSignificant:
                return userDataLeastSignificant;
            case MostSignificant:
                return userDataMostSignificant;
            default:
                throw new IllegalArgumentException("Invalid byte part");
        }
    }

    /**
     * <p>
     *
     * @param leastSignificant
     * @param mostSignificant
     */
    public void setUserData(long leastSignificant, long mostSignificant) {
        this.userDataLeastSignificant = leastSignificant;
        this.userDataMostSignificant = mostSignificant;
    }

    /**
     * <p>
     * May be zero, null values are converted to zero.
     *
     * @param userData
     * @throws IllegalArgumentException if {@code userData} is not 16 bytes long.
     */
    public void setUserData(final byte[] userData) {
        if (userData == null) {
            this.userDataLeastSignificant = 0L;
            this.userDataMostSignificant = 0L;
        } else {
            this.userDataLeastSignificant = UInt128.getLong(userData, Bytes.LeastSignificant);
            this.userDataMostSignificant = UInt128.getLong(userData, Bytes.MostSignificant);
        }
    }

    /**
     * <p>
     *
     * @return
     */
    public byte[] getPendingId() {
        return UInt128.toBytes(pendingIdLeastSignificant, pendingIdMostSignificant);
    }

    /**
     * <p>
     *
     * @return
     */
    public long getPendingId(Bytes part) {
        switch (part) {
            case LeastSignificant:
                return pendingIdLeastSignificant;
            case MostSignificant:
                return pendingIdMostSignificant;
            default:
                throw new IllegalArgumentException("Invalid byte part");
        }
    }

    /**
     * <p>
     *
     * @param leastSignificant
     * @param mostSignificant
     */
    public void setPendingId(long leastSignificant, long mostSignificant) {
        this.pendingIdLeastSignificant = leastSignificant;
        this.pendingIdMostSignificant = mostSignificant;
    }

    /**
     * <p>
     * May be zero, null values are converted to zero.
     *
     * @param userData
     * @throws IllegalArgumentException if {@code userData} is not 16 bytes long.
     */
    public void setPendingId(final byte[] pendingId) {
        if (pendingId == null) {
            this.pendingIdLeastSignificant = 0L;
            this.pendingIdMostSignificant = 0L;
        } else {
            this.pendingIdLeastSignificant = UInt128.getLong(pendingId, Bytes.LeastSignificant);
            this.pendingIdMostSignificant = UInt128.getLong(pendingId, Bytes.MostSignificant);
        }
    }

    /**
     *
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * @param timeout A 64-bit integer.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * Gets an identifier used to enforce transfers between the same ledger.
     *
     * @return a 32-bit integer.
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
     * @param ledger a 32-bit integer defined by the user.
     */
    public void setLedger(int ledger) {
        this.ledger = ledger;
    }

    /**
     * Gets a reason for the transfer.
     *
     * @return a 16-bit unsigned integer.
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
     * @param code a 16-bit unsigned integer defined by the user.
     * @throws IllegalArgumentException if code is negative or greater than 65535.
     */
    public void setCode(int code) {
        if (code < 0 || code > Character.MAX_VALUE)
            throw new IllegalArgumentException("Code must be a 16-bit unsigned integer");

        this.code = (short) code;
    }

    /**
     * Gets the behavior during transfers.
     *
     * @see com.tigerbeetle.TransferFlags
     * @return a 16-bit unsigned integer bit mask.
     */
    public int getFlags() {
        return flags;
    }

    /**
     * Sets the behavior during transfers.
     * <p>
     *
     * @see com.tigerbeetle.TransferFlags
     * @param flags a 16-bit unsigned integer bit mask.
     * @throws IllegalArgumentException if flags is negative or greater than 65535.
     */
    public void setFlags(int flags) {
        if (flags < 0 || flags > Character.MAX_VALUE)
            throw new IllegalArgumentException("Flags must be a 16-bit unsigned integer");

        this.flags = (short) flags;
    }

    /**
     *
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return A 64-bit integer.
     */
    public long getAmount() {
        return amount;
    }

    /**
     *
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @param amount a 64-bit integer.
     */
    public void setAmount(long amount) {
        this.amount = amount;
    }

    /**
     * Time transfer was created.
     * <p>
     * UNIX timestamp in nanoseconds.
     *
     * @return a 64-bit integer.
     */
    public long getTimestamp() {
        return timestamp;
    }

    void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    void copyTo(ByteBuffer ptr) {
        ptr.putLong(idLeastSignificant);
        ptr.putLong(idMostSignificant);
        ptr.putLong(debitAccountIdLeastSignificant);
        ptr.putLong(debitAccountIdMostSignificant);
        ptr.putLong(creditAccountIdLeastSignificant);
        ptr.putLong(creditAccountIdMostSignificant);
        ptr.putLong(userDataLeastSignificant);
        ptr.putLong(userDataMostSignificant);
        ptr.put(Struct.RESERVED);
        ptr.putLong(pendingIdLeastSignificant);
        ptr.putLong(pendingIdMostSignificant);
        ptr.putLong(timeout);
        ptr.putInt(ledger);
        ptr.putShort(code);
        ptr.putShort(flags);
        ptr.putLong(amount);
        ptr.putLong(timestamp);
    }

}
