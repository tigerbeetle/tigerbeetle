package com.tigerbeetle;

public final class QueryFilter {

    // @formatter:off
    /*
    * Summary:
    *
    * Wraps the `QueryFilterBatch` auto-generated binding in a single-item batch.
    * Since `queryAccounts()` and `queryTransfers()` expects only one item, we avoid
    * exposing the `Batch` class externally.
    *
    * This is an ad-hoc feature meant to be replaced by a proper querying API shortly,
    * therefore, it is not worth the effort to modify the binding generator to emit single-item batchs.
    *
    */
    // @formatter:on

    QueryFilterBatch batch;

    public QueryFilter() {
        this.batch = new QueryFilterBatch(1);
        this.batch.add();
    }

    /**
     * @return an array of 16 bytes representing the 128-bit value.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter#user_data_128">user_data_128</a>
     */
    public byte[] getUserData128() {
        return this.batch.getUserData128();
    }

    /**
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be
     *        retrieved.
     * @return a {@code long} representing the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter#user_data_128">user_data_128</a>
     */
    public long getUserData128(final UInt128 part) {
        return this.batch.getUserData128(part);
    }

    /**
     * @param userData128 an array of 16 bytes representing the 128-bit value.
     * @throws IllegalArgumentException if {@code userData128} is not 16 bytes long.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter#user_data_128">user_data_128</a>
     */
    public void setUserData128(final byte[] userData128) {
        this.batch.setUserData128(userData128);
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @param mostSignificant a {@code long} representing the last 8 bytes of the 128-bit value.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter#user_data_128">user_data_128</a>
     */
    public void setUserData128(final long leastSignificant, final long mostSignificant) {
        this.batch.setUserData128(leastSignificant, mostSignificant);
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter#user_data_128">user_data_128</a>
     */
    public void setUserData128(final long leastSignificant) {
        this.batch.setUserData128(leastSignificant);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter#user_data_64">user_data_64</a>
     */
    public long getUserData64() {
        return this.batch.getUserData64();
    }

    /**
     * @param userData64
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter#user_data_64">user_data_64</a>
     */
    public void setUserData64(final long userData64) {
        this.batch.setUserData64(userData64);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter#user_data_32">user_data_32</a>
     */
    public int getUserData32() {
        return this.batch.getUserData32();
    }

    /**
     * @param userData32
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter#user_data_32">user_data_32</a>
     */
    public void setUserData32(final int userData32) {
        this.batch.setUserData32(userData32);
    }

    /**
     * @see <a href="https://docs.tigerbeetle.com/reference/query_filter#ledger">ledger</a>
     */
    public int getLedger() {
        return this.batch.getLedger();
    }

    /**
     * @param ledger
     * @see <a href="https://docs.tigerbeetle.com/reference/query_filter#ledger">ledger</a>
     */
    public void setLedger(final int ledger) {
        this.batch.setLedger(ledger);
    }

    /**
     * @see <a href="https://docs.tigerbeetle.com/reference/query_filter#code">code</a>
     */
    public int getCode() {
        return this.batch.getCode();
    }

    /**
     * @param code
     * @see <a href="https://docs.tigerbeetle.com/reference/query_filter#code">code</a>
     */
    public void setCode(final int code) {
        this.batch.setCode(code);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter-filter#timestamp_min">timestamp_min</a>
     */
    public long getTimestampMin() {
        return batch.getTimestampMin();
    }

    /**
     * @param timestamp
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter-filter#timestamp_min">timestamp_min</a>
     */
    public void setTimestampMin(final long timestamp) {
        batch.setTimestampMin(timestamp);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter-filter#timestamp_max">timestamp_max</a>
     */
    public long getTimestampMax() {
        return batch.getTimestampMax();
    }

    /**
     * @param timestamp
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter-filter#timestamp_max">timestamp_max</a>
     */
    public void setTimestampMax(final long timestamp) {
        batch.setTimestampMax(timestamp);
    }

    /**
     * @see <a href= "https://docs.tigerbeetle.com/reference/query_filter-filter#limit">limit</a>
     */
    public int getLimit() {
        return batch.getLimit();
    }

    /**
     * @param limit
     * @see <a href= "https://docs.tigerbeetle.com/reference/query_filter-filter#limit">limit</a>
     */
    public void setLimit(final int limit) {
        batch.setLimit(limit);
    }

    /**
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter-filter#flagsreversed">reversed</a>
     */
    public boolean getReversed() {
        return getFlags(QueryFilterFlags.REVERSED);
    }

    /**
     * @param value
     * @see <a href=
     *      "https://docs.tigerbeetle.com/reference/query_filter-filter#flagsreversed">reversed</a>
     */
    public void setReversed(boolean value) {
        setFlags(QueryFilterFlags.REVERSED, value);
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
