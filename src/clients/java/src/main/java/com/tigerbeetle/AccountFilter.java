package com.tigerbeetle;

public final class AccountFilter {

    // @formatter:off
    /*
    * Summary:
    *
    * Wraps the `AccountFilterBatch` auto-generated binding in a single-item batch.
    * Since `getAccountTransfers()` expects only one item, we avoid exposing the `Batch` class externally.
    *
    * This is an ad-hoc feature meant to be replaced by a proper querying API shortly,
    * therefore, it is not worth the effort to modify the binding generator to emit single-item batchs.
    *
    */
    // @formatter:on

    AccountFilterBatch batch;

    public AccountFilter() {
        this.batch = new AccountFilterBatch(1);
        this.batch.add();
    }

    /**
     * @return an array of 16 bytes representing the 128-bit value.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#account_id">account_id</a>
     */
    public byte[] getAccountId() {
        return batch.getAccountId();
    }

    /**
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be
     *        retrieved.
     * @return a {@code long} representing the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#account_id">account_id</a>
     */
    public long getAccountId(final UInt128 part) {
        return batch.getAccountId(part);
    }

    /**
     * @param accountId an array of 16 bytes representing the 128-bit value.
     * @throws IllegalArgumentException if {@code id} is not 16 bytes long.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#account_id">account_id</a>
     */
    public void setAccountId(final byte[] accountId) {
        batch.setAccountId(accountId);
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @param mostSignificant a {@code long} representing the last 8 bytes of the 128-bit value.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#account_id">account_id</a>
     */
    public void setAccountId(final long leastSignificant, final long mostSignificant) {
        batch.setAccountId(leastSignificant, mostSignificant);
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#account_id">account_id</a>
     */
    public void setAccountId(final long leastSignificant) {
        batch.setAccountId(leastSignificant);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#timestamp_min">timestamp_min</a>
     */
    public long getTimestampMin() {
        return batch.getTimestampMin();
    }

    /**
     * @param timestamp
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#timestamp_min">timestamp_min</a>
     */
    public void setTimestampMin(final long timestamp) {
        batch.setTimestampMin(timestamp);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#timestamp_max">timestamp_max</a>
     */
    public long getTimestampMax() {
        return batch.getTimestampMax();
    }

    /**
     * @param timestamp
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#timestamp_max">timestamp_max</a>
     */
    public void setTimestampMax(final long timestamp) {
        batch.setTimestampMax(timestamp);
    }

    /**
     * @see <a href= "https://docs.tigerbeetle.com/reference/account-filter#limit">limit</a>
     */
    public int getLimit() {
        return batch.getLimit();
    }

    /**
     * @param limit
     * @see <a href= "https://docs.tigerbeetle.com/reference/account-filter#limit">limit</a>
     */
    public void setLimit(final int limit) {
        batch.setLimit(limit);
    }

    /**
     * @see <a href= "https://docs.tigerbeetle.com/reference/account-filter#flagsdebits">debits</a>
     */
    public boolean getDebits() {
        return getFlags(AccountFilterFlags.DEBITS);
    }

    /**
     * @param value
     * @see <a href= "https://docs.tigerbeetle.com/reference/account-filter#flagsdebits">debits</a>
     */
    public void setDebits(boolean value) {
        setFlags(AccountFilterFlags.DEBITS, value);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#flagscredits">credits</a>
     */
    public boolean getCredits() {
        return getFlags(AccountFilterFlags.CREDITS);
    }

    /**
     * @param value
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#flagscredits">credits</a>
     */
    public void setCredits(boolean value) {
        setFlags(AccountFilterFlags.CREDITS, value);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#flagsreversed">reversed</a>
     */
    public boolean getReversed() {
        return getFlags(AccountFilterFlags.REVERSED);
    }

    /**
     * @param value
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/account-filter#flagsreversed">reversed</a>
     */
    public void setReversed(boolean value) {
        setFlags(AccountFilterFlags.REVERSED, value);
    }

    boolean getFlags(final int flag) {
        final var value = batch.getFlags();
        return (value & flag) != 0;
    }

    void setFlags(final int flag, final boolean enabled) {
        var value = batch.getFlags();
        if (enabled)
            value |= flag;
        else
            value &= ~flag;
        batch.setFlags(value);
    }
}
