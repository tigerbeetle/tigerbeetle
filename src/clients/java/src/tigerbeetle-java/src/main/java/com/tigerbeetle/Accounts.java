package com.tigerbeetle;

import java.nio.ByteBuffer;

public final class Accounts extends Batch {

    interface Struct {

        public static final int Id = 0;
        public static final int UserData = 16;
        public static final int Reserved = 32;
        public static final int Ledger = 80;
        public static final int Code = 84;
        public static final int Flags = 86;
        public static final int DebitsPending = 88;
        public static final int DebitsPosted = 96;
        public static final int CreditsPending = 104;
        public static final int CreditsPosted = 112;
        public static final int Timestamp = 120;

        public static final int SIZE = 128;
    }

    static final Accounts EMPTY = new Accounts(0);

    public Accounts(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    Accounts(final ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }

    @Override
    public void add() {
        super.add();
    }

    /**
     * <p>
     *
     * @return
     * @throws IllegalStateException
     */
    public byte[] getId() {
        return getUInt128(at(Struct.Id));
    }

    /**
     * <p>
     *
     * @return
     */
    public long getId(final UInt128 part) {
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
    public byte[] getUserData() {
        return getUInt128(at(Struct.UserData));
    }

    /**
     * <p>
     *
     * @return
     */
    public long getUserData(final UInt128 part) {
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
     * @return a 16-bit unsigned integer bit mask.
     * @see com.tigerbeetle.AccountFlags
     */
    public int getFlags() {
        return getUInt16(at(Struct.Flags));
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
    public void setFlags(final int flags) {
        putUInt16(at(Struct.Flags), flags);
    }

    /**
     * Amount of pending debits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     */
    public long getDebitsPending() {
        return getUInt64(at(Struct.DebitsPending));
    }

    void setDebitsPending(final long debitsPending) {
        putUInt64(at(Struct.DebitsPending), debitsPending);
    }

    /**
     * Amount of non-pending debits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     */
    public long getDebitsPosted() {
        return getUInt64(at(Struct.DebitsPosted));
    }

    void setDebitsPosted(final long debitsPosted) {
        putUInt64(at(Struct.DebitsPosted), debitsPosted);
    }

    /**
     * Amount of pending credits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     */
    public long getCreditsPending() {
        return getUInt64(at(Struct.CreditsPending));
    }

    void setCreditsPending(final long creditsPending) {
        putUInt64(at(Struct.CreditsPending), creditsPending);
    }

    /**
     * Amount of non-pending credits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     */
    public long getCreditsPosted() {
        return getUInt64(at(Struct.CreditsPosted));
    }

    void setCreditsPosted(final long creditsPosted) {
        putUInt64(at(Struct.CreditsPosted), creditsPosted);
    }

    /**
     * Time account was created.
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
