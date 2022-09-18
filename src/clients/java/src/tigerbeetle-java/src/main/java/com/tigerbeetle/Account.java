package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * TigerBeetle uses fixed-size data structures to represent all data, including accounts. Account
 * fields cannot be changed by the user after creation. However, debits and credits fields are
 * modified by TigerBeetle as transfers happen to and from the account.
 */
public final class Account {

    static final class Struct {
        public static final int SIZE = 128;
        public static final byte[] RESERVED = new byte[48];
    }

    private static final UUID ZERO = new UUID(0, 0);

    private UUID id;
    private UUID userData;
    private int ledger;
    private short code;
    private short flags;
    private long creditsPosted;
    private long creditsPending;
    private long debitsPosted;
    private long debitsPending;
    private long timestamp;

    public Account() {
        id = ZERO;
        userData = ZERO;
        ledger = 0;
        code = 0;
        flags = AccountFlags.NONE;
        creditsPosted = 0;
        creditsPending = 0;
        debitsPosted = 0;
        debitsPending = 0;
        timestamp = 0;
    }

    Account(ByteBuffer ptr) {
        id = Batch.uuidFromBuffer(ptr);
        userData = Batch.uuidFromBuffer(ptr);
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
     * Gets an identifier for this account, defined by the user.
     *
     * @return An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any other unique
     *         128-bit integer.
     */
    public UUID getId() {
        return id;
    }

    /**
     * Sets an identifier for this account, defined by the user.
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
     * Gets the secondary identifier to link this account to an external entity.
     *
     * @return An {@link java.util.UUID} representing an integer-encoded UUIDv4 or any other unique
     *         128-bit integer.
     */
    public UUID getUserData() {
        return userData;
    }

    /**
     * Sets the secondary identifier to link this account to an external entity.
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
     * @param ledger A 32-bit integer defined by the user
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
            throw new IllegalArgumentException("Code must be a 16-bits unsigned integer");

        this.code = (short) code;
    }

    /**
     * Gets the behavior during transfers.
     *
     * @return A 16-bits unsigned integer bit mask
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
     * @param flags A 16-bits unsigned integer bit mask
     * @throws IllegalArgumentException if flags is negative or greater than 65535.
     */
    public void setFlags(int flags) {
        if (flags < 0 || flags > Character.MAX_VALUE)
            throw new IllegalArgumentException("Flags must be a 16-bits unsigned integer");

        this.flags = (short) flags;
    }

    /**
     * Amount of pending debits.
     * <p>
     * Must always be interpreted as a positive integer.
     *
     * @return A 64-bits integer
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
     * @return A 64-bits integer
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
     * @return A 64-bits integer
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
     * @return A 64-bits integer
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
     * @return A 64-bits integer
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
        ptr.putLong(userData.getLeastSignificantBits());
        ptr.putLong(userData.getMostSignificantBits());
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
