package com.tigerbeetle;

import java.nio.ByteBuffer;
/**
 * A {@link Batch batch} of accounts.
 */
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

    /**
     * Constructs an empty batch of accounts with the desired maximum capacity.
     * <p>
     * Once created, an instance cannot be resized, however it may contain any number of accounts
     * between zero and its {@link #getCapacity capacity}.
     *
     * @param capacity the maximum capacity.
     *
     * @throws IllegalArgumentException if capacity is negative.
     */
    public Accounts(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    Accounts(final ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }

    /**
     * Adds a new account at the end of this batch.
     * <p>
     * If successfully, moves the current {@link #setPosition position} to the newly created
     * account.
     *
     * @throws IllegalStateException if this batch is read-only.
     * @throws IndexOutOfBoundsException if exceeds the batch's capacity.
     */
    @Override
    public void add() {
        super.add();
    }

    /**
     * Gets a unique, client-defined identifier for the account.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getId() {
        return getUInt128(at(Struct.Id));
    }

    /**
     * Gets a unique, client-defined identifier for the account.
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
     * Sets a unique, client-defined identifier for the account.
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
     * Sets a unique, client-defined identifier for the account.
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
     * Gets an optional secondary identifier to link this account to an external entity.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getUserData() {
        return getUInt128(at(Struct.UserData));
    }

    /**
     * Gets an optional secondary identifier to link this account to an external entity.
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
     * Sets an optional secondary identifier to link this account to an external entity.
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
     * Sets an optional secondary identifier to link this account to an external entity.
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
     * Gets an identifier that partitions the sets of accounts that can transact.
     *
     * @return a 32-bit integer.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public int getLedger() {
        return getUInt32(at(Struct.Ledger));
    }

    /**
     * Sets an identifier that partitions the sets of accounts that can transact.
     * <p>
     * Must not be zero.
     *
     * @param ledger a 32-bit integer.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setLedger(final int ledger) {
        putUInt32(at(Struct.Ledger), ledger);
    }

    /**
     * Gets a user-defined enum denoting the category of the account.
     *
     * @return a 16-bit unsigned integer.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public int getCode() {
        return getUInt16(at(Struct.Code));
    }

    /**
     * Sets a user-defined enum denoting the category of the account.
     * <p>
     * Must not be zero.
     *
     * @param code a 16-bit unsigned integer.
     * @throws IllegalArgumentException if code is negative or greater than 65535.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setCode(final int code) {
        putUInt16(at(Struct.Code), code);
    }

    /**
     * Gets a bitfield that toggles additional behavior.
     *
     * @return a 16-bit unsigned integer bit mask.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see com.tigerbeetle.AccountFlags
     */
    public int getFlags() {
        return getUInt16(at(Struct.Flags));
    }

    /**
     * Sets a bitfield that toggles additional behavior.
     *
     * @see com.tigerbeetle.AccountFlags
     * @param flags a 16-bit unsigned integer bit mask.
     * @throws IllegalArgumentException if flags is negative or greater than 65535.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     */
    public void setFlags(final int flags) {
        putUInt16(at(Struct.Flags), flags);
    }

    /**
     * Gets the amount of debits reserved by pending transfers.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getDebitsPending() {
        return getUInt64(at(Struct.DebitsPending));
    }

    void setDebitsPending(final long debitsPending) {
        putUInt64(at(Struct.DebitsPending), debitsPending);
    }

    /**
     * Gets the amount of posted debits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getDebitsPosted() {
        return getUInt64(at(Struct.DebitsPosted));
    }

    void setDebitsPosted(final long debitsPosted) {
        putUInt64(at(Struct.DebitsPosted), debitsPosted);
    }

    /**
     * Gets the amount of pending credits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getCreditsPending() {
        return getUInt64(at(Struct.CreditsPending));
    }

    void setCreditsPending(final long creditsPending) {
        putUInt64(at(Struct.CreditsPending), creditsPending);
    }

    /**
     * Gets the mmount of posted credits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return a 64-bit integer.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public long getCreditsPosted() {
        return getUInt64(at(Struct.CreditsPosted));
    }

    void setCreditsPosted(final long creditsPosted) {
        putUInt64(at(Struct.CreditsPosted), creditsPosted);
    }

    /**
     * Gets the time the account was created.
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
