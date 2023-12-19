package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class AccountTransfersTest {

    @Test
    public void testDefaultValues() {
        var accountTransfers = new AccountTransfers();
        assertEquals(0L, accountTransfers.getAccountId(UInt128.LeastSignificant));
        assertEquals(0L, accountTransfers.getAccountId(UInt128.MostSignificant));
        assertEquals(0L, accountTransfers.getTimestamp());
        assertEquals(0, accountTransfers.getLimit());
        assertEquals(false, accountTransfers.getDebits());
        assertEquals(false, accountTransfers.getCredits());
        assertEquals(false, accountTransfers.getReversed());
    }

    @Test
    public void testAccountId() {
        var accountTransfers = new AccountTransfers();

        accountTransfers.setAccountId(100, 200);
        assertEquals(100L, accountTransfers.getAccountId(UInt128.LeastSignificant));
        assertEquals(200L, accountTransfers.getAccountId(UInt128.MostSignificant));
    }

    @Test
    public void testAccountIdLong() {
        var accountTransfers = new AccountTransfers();

        accountTransfers.setAccountId(100);
        assertEquals(100L, accountTransfers.getAccountId(UInt128.LeastSignificant));
        assertEquals(0L, accountTransfers.getAccountId(UInt128.MostSignificant));
    }

    @Test
    public void testAccountIdIdAsBytes() {
        var accountTransfers = new AccountTransfers();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        accountTransfers.setAccountId(id);
        assertArrayEquals(id, accountTransfers.getAccountId());
    }

    @Test
    public void testAccountIdNull() {
        var accountTransfers = new AccountTransfers();

        byte[] id = null;
        accountTransfers.setAccountId(id);

        assertArrayEquals(new byte[16], accountTransfers.getAccountId());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAccountIdInvalid() {
        var accountTransfers = new AccountTransfers();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        accountTransfers.setAccountId(id);
        assert false;
    }

    @Test
    public void testTimestamp() {
        var accountTransfers = new AccountTransfers();

        accountTransfers.setTimestamp(100L);
        assertEquals(100, accountTransfers.getTimestamp());
    }

    @Test
    public void testLimit() {
        var accountTransfers = new AccountTransfers();

        accountTransfers.setLimit(30);
        assertEquals(30, accountTransfers.getLimit());
    }

    @Test
    public void testFlags() {
        // Debits
        {
            var accountTransfers = new AccountTransfers();
            accountTransfers.setDebits(true);
            assertEquals(true, accountTransfers.getDebits());
            assertEquals(false, accountTransfers.getCredits());
            assertEquals(false, accountTransfers.getReversed());
        }

        // Credits
        {
            var accountTransfers = new AccountTransfers();
            accountTransfers.setCredits(true);
            assertEquals(false, accountTransfers.getDebits());
            assertEquals(true, accountTransfers.getCredits());
            assertEquals(false, accountTransfers.getReversed());
        }

        // Direction
        {
            var accountTransfers = new AccountTransfers();
            accountTransfers.setReversed(true);
            assertEquals(false, accountTransfers.getDebits());
            assertEquals(false, accountTransfers.getCredits());
            assertEquals(true, accountTransfers.getReversed());
        }
    }

}
