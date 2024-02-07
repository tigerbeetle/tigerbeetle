package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class AccountFilterTest {

    @Test
    public void testDefaultValues() {
        final var accountTransfers = new AccountFilter();
        assertEquals(0L, accountTransfers.getAccountId(UInt128.LeastSignificant));
        assertEquals(0L, accountTransfers.getAccountId(UInt128.MostSignificant));
        assertEquals(0L, accountTransfers.getTimestampMin());
        assertEquals(0L, accountTransfers.getTimestampMax());
        assertEquals(0, accountTransfers.getLimit());
        assertEquals(false, accountTransfers.getDebits());
        assertEquals(false, accountTransfers.getCredits());
        assertEquals(false, accountTransfers.getReversed());
    }

    @Test
    public void testAccountId() {
        final var accountTransfers = new AccountFilter();

        accountTransfers.setAccountId(100, 200);
        assertEquals(100L, accountTransfers.getAccountId(UInt128.LeastSignificant));
        assertEquals(200L, accountTransfers.getAccountId(UInt128.MostSignificant));
    }

    @Test
    public void testAccountIdLong() {
        final var accountTransfers = new AccountFilter();

        accountTransfers.setAccountId(100);
        assertEquals(100L, accountTransfers.getAccountId(UInt128.LeastSignificant));
        assertEquals(0L, accountTransfers.getAccountId(UInt128.MostSignificant));
    }

    @Test
    public void testAccountIdIdAsBytes() {
        final var accountTransfers = new AccountFilter();

        final var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        accountTransfers.setAccountId(id);
        assertArrayEquals(id, accountTransfers.getAccountId());
    }

    @Test
    public void testAccountIdNull() {
        final var accountTransfers = new AccountFilter();

        final byte[] id = null;
        accountTransfers.setAccountId(id);

        assertArrayEquals(new byte[16], accountTransfers.getAccountId());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAccountIdInvalid() {
        final var accountTransfers = new AccountFilter();

        final var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        accountTransfers.setAccountId(id);
        assert false;
    }

    @Test
    public void testTimestampMin() {
        final var accountTransfers = new AccountFilter();

        accountTransfers.setTimestampMin(100L);
        assertEquals(100, accountTransfers.getTimestampMin());
    }

    @Test
    public void testTimestampMax() {
        final var accountTransfers = new AccountFilter();

        accountTransfers.setTimestampMax(100L);
        assertEquals(100, accountTransfers.getTimestampMax());
    }

    @Test
    public void testLimit() {
        final var accountTransfers = new AccountFilter();

        accountTransfers.setLimit(30);
        assertEquals(30, accountTransfers.getLimit());
    }

    @Test
    public void testFlags() {
        // Debits
        {
            final var accountTransfers = new AccountFilter();
            accountTransfers.setDebits(true);
            assertEquals(true, accountTransfers.getDebits());
            assertEquals(false, accountTransfers.getCredits());
            assertEquals(false, accountTransfers.getReversed());
        }

        // Credits
        {
            final var accountTransfers = new AccountFilter();
            accountTransfers.setCredits(true);
            assertEquals(false, accountTransfers.getDebits());
            assertEquals(true, accountTransfers.getCredits());
            assertEquals(false, accountTransfers.getReversed());
        }

        // Direction
        {
            final var accountTransfers = new AccountFilter();
            accountTransfers.setReversed(true);
            assertEquals(false, accountTransfers.getDebits());
            assertEquals(false, accountTransfers.getCredits());
            assertEquals(true, accountTransfers.getReversed());
        }
    }

}
