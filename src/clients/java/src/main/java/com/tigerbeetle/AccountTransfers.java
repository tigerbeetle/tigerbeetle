package com.tigerbeetle;

public final class AccountTransfers {

    // @formatter:off
    /*
    * Summary:
    *
    * Wraps the `GetAccountTransfersBatch` auto-generated binding in a single-item batch.
    * Since `getAccountTransfers()` expects only one item, we avoid exposing the `Batch` class externally.
    *
    * This is an ad-hoc feature meant to be replaced by a proper querying API shortly,
    * therefore, it is not worth the effort to modify the binding generator to emit single-item batchs.
    *
    */
    // @formatter:on

    GetAccountTransfersBatch batch;

    public AccountTransfers() {
        this.batch = new GetAccountTransfersBatch(1);
        this.batch.add();
    }

    /**
     * @return an array of 16 bytes representing the 128-bit value.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#account_id">account_id</a>
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
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#account_id">account_id</a>
     */
    public long getAccountId(final UInt128 part) {
        return batch.getAccountId(part);
    }

    /**
     * @param accountId an array of 16 bytes representing the 128-bit value.
     * @throws IllegalArgumentException if {@code id} is not 16 bytes long.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#account_id">account_id</a>
     */
    public void setAccountId(final byte[] accountId) {
        batch.setAccountId(accountId);
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @param mostSignificant a {@code long} representing the last 8 bytes of the 128-bit value.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#account_id">account_id</a>
     */
    public void setAccountId(final long leastSignificant, final long mostSignificant) {
        batch.setAccountId(leastSignificant, mostSignificant);
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#account_id">account_id</a>
     */
    public void setAccountId(final long leastSignificant) {
        batch.setAccountId(leastSignificant);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#timestamp">timestamp</a>
     */
    public long getTimestamp() {
        return batch.getTimestamp();
    }

    /**
     * @param timestamp
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#timestamp">timestamp</a>
     */
    public void setTimestamp(final long timestamp) {
        batch.setTimestamp(timestamp);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#limit">limit</a>
     */
    public int getLimit() {
        return batch.getLimit();
    }

    /**
     * @param limit
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#limit">limit</a>
     */
    public void setLimit(final int limit) {
        batch.setLimit(limit);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#flagsdebits">debits</a>
     */
    public boolean getDebits() {
        return getFlags(GetAccountTransfersFlags.DEBITS);
    }

    /**
     * @param value
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#flagsdebits">debits</a>
     */
    public void setDebits(boolean value) {
        setFlags(GetAccountTransfersFlags.DEBITS, value);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#flagscredits">credits</a>
     */
    public boolean getCredits() {
        return getFlags(GetAccountTransfersFlags.CREDITS);
    }

    /**
     * @param value
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#flagscredits">credits</a>
     */
    public void setCredits(boolean value) {
        setFlags(GetAccountTransfersFlags.CREDITS, value);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#flagsreversed">reversed</a>
     */
    public boolean getReversed() {
        return getFlags(GetAccountTransfersFlags.REVERSED);
    }

    /**
     * @param value
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/operations/get_account_transfers#flagsreversed">reversed</a>
     */
    public void setReversed(boolean value) {
        setFlags(GetAccountTransfersFlags.REVERSED, value);
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
