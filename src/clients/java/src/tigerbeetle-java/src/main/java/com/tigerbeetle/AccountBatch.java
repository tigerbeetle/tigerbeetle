package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A {@link Batch batch} of accounts.
 *
 * @see <a href="https://docs.tigerbeetle.com/reference/accounts">TigerBeetle Docs</a>.
 */
public final class AccountBatch extends Batch {

    interface Struct {

        int Id = 0;
        int UserData = 16;
        int Reserved = 32;
        int Ledger = 80;
        int Code = 84;
        int Flags = 86;
        int DebitsPending = 88;
        int DebitsPosted = 96;
        int CreditsPending = 104;
        int CreditsPosted = 112;
        int Timestamp = 120;

        int SIZE = 128;
    }

    static final AccountBatch EMPTY = new AccountBatch(0);

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
    public AccountBatch(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    AccountBatch(final ByteBuffer buffer) {
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#id">id</a>.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getId() {
        return getUInt128(at(Struct.Id));
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#id">id</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/accounts#id">id</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/accounts#id">id</a>.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#user_data">user data</a>.
     *
     * @return an array of 16 bytes representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public byte[] getUserData() {
        return getUInt128(at(Struct.UserData));
    }

    /**
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#user_data">user data</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/accounts#user_data">user data</a>.
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
     * Sets the <a href="https://docs.tigerbeetle.com/reference/accounts#user_data">user data</a>.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#ledger">ledger</a>.
     *
     * @return a 32-bit integer.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public int getLedger() {
        return getUInt32(at(Struct.Ledger));
    }

    /**
     * Sets the <a href="https://docs.tigerbeetle.com/reference/accounts#ledger">ledger</a>.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#code">code</a>.
     *
     * @return a 16-bit unsigned integer.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     */
    public int getCode() {
        return getUInt16(at(Struct.Code));
    }

    /**
     * Sets the <a href="https://docs.tigerbeetle.com/reference/accounts#code">code</a>.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#flags">flags</a>.
     *
     * @return a 16-bit unsigned integer bit mask.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see com.tigerbeetle.AccountFlags
     */
    public int getFlags() {
        return getUInt16(at(Struct.Flags));
    }

    /**
     * Sets the <a href="https://docs.tigerbeetle.com/reference/accounts#flags">flags</a>.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#debits_pending">debits
     * pending</a>.
     * <p>
     * Must always be interpreted as an unsigned integer.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#debits_posted">debits
     * posted</a>.
     * <p>
     * Must always be interpreted as an unsigned integer.
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
     * Gets the <a href=
     * "https://docs.tigerbeetle.com/reference/accounts#credits_pending">credits_pending</a>.
     * <p>
     * Must always be interpreted as an unsigned integer.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#credits_posted">credits
     * posted</a>.
     * <p>
     * Must always be interpreted as an unsigned integer.
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
     * Gets the <a href="https://docs.tigerbeetle.com/reference/accounts#timestamp">timestamp</a>
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
