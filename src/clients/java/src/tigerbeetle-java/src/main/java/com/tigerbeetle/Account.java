package com.tigerbeetle;

import java.nio.ByteBuffer;
import com.tigerbeetle.UInt128.Bytes;

/**
 * Account.
 * <p>
 * Fields cannot be changed by the user after creation. However, debits and credits fields are
 * modified by TigerBeetle as transfers happen to and from the account.
 */
public final class Account {

    interface Struct {
        public static final int SIZE = 128;
        public static final byte[] RESERVED = new byte[48];
    }

    private long idLeastSignificant;
    private long idMostSignificant;
    private long userDataLeastSignificant;
    private long userDataMostSignificant;
    private int ledger;
    private short code;
    private short flags;
    private long creditsPosted;
    private long creditsPending;
    private long debitsPosted;
    private long debitsPending;
    private long timestamp;

    public Account() {
        idLeastSignificant = 0L;
        idMostSignificant = 0L;
        userDataLeastSignificant = 0L;
        userDataMostSignificant = 0L;
        ledger = 0;
        code = 0;
        flags = AccountFlags.NONE;
        creditsPosted = 0L;
        creditsPending = 0L;
        debitsPosted = 0L;
        debitsPending = 0L;
        timestamp = 0L;
    }

    Account(ByteBuffer ptr) {
        idLeastSignificant = ptr.getLong();
        idMostSignificant = ptr.getLong();
        userDataLeastSignificant = ptr.getLong();
        userDataMostSignificant = ptr.getLong();
        ptr = ptr.position(ptr.position() + Struct.RESERVED.length);
        ledger = ptr.getInt();
        code = ptr.getShort();
        flags = ptr.getShort();
        debitsPending = ptr.getLong();
        debitsPosted = ptr.getLong();
        creditsPending = ptr.getLong();
        creditsPosted = ptr.getLong();
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
     * @param id
     * @throws NullPointerException if {@code id} is null.
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
     * @return a 16-bit unsigned integer bit mask.
     * @see com.tigerbeetle.AccountFlags
     */
    public int getFlags() {
        return flags;
    }

    /**
     * Sets the behavior during transfers.
     * <p>
     * Example: {@code AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS} to force debts not to exceed
     * credits.
     *
     * @see com.tigerbeetle.AccountFlags
     * @param flags a 16-bit unsigned integer bit mask.
     * @throws IllegalArgumentException if flags is negative or greater than 65535.
     */
    public void setFlags(int flags) {
        if (flags < 0 || flags > Character.MAX_VALUE)
            throw new IllegalArgumentException("Flags must be a 16-bit unsigned integer");

        this.flags = (short) flags;
    }

    /**
     * Amount of pending debits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     */
    public long getDebitsPending() {
        return debitsPending;
    }

    void setDebitsPending(long debitsPending) {
        this.debitsPending = debitsPending;
    }

    /**
     * Amount of non-pending debits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     */
    public long getDebitsPosted() {
        return debitsPosted;
    }

    void setDebitsPosted(long debitsPosted) {
        this.debitsPosted = debitsPosted;
    }

    /**
     * Amount of pending credits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     */
    public long getCreditsPending() {
        return creditsPending;
    }

    void setCreditsPending(long creditsPending) {
        this.creditsPending = creditsPending;
    }

    /**
     * Amount of non-pending credits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     */
    public long getCreditsPosted() {
        return creditsPosted;
    }

    void setCreditsPosted(long creditsPosted) {
        this.creditsPosted = creditsPosted;
    }

    /**
     * Time account was created.
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
        ptr.putLong(userDataLeastSignificant);
        ptr.putLong(userDataMostSignificant);
        ptr.put(Struct.RESERVED);
        ptr.putInt(ledger);
        ptr.putShort(code);
        ptr.putShort(flags);
        ptr.putLong(debitsPending);
        ptr.putLong(debitsPosted);
        ptr.putLong(creditsPending);
        ptr.putLong(creditsPosted);
        ptr.putLong(timestamp);
    }
}
