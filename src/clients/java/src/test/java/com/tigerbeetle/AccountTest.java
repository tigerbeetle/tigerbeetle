package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class AccountTest {

    @Test
    public void testDefaultValues() {
        var accounts = new AccountBatch(1);
        accounts.add();

        assertEquals(0L, accounts.getId(UInt128.LeastSignificant));
        assertEquals(0L, accounts.getId(UInt128.MostSignificant));
        assertEquals(0L, accounts.getUserData(UInt128.LeastSignificant));
        assertEquals(0L, accounts.getUserData(UInt128.MostSignificant));
        assertEquals(0, accounts.getLedger());
        assertEquals(AccountFlags.NONE, accounts.getFlags());
        assertEquals((long) 0, accounts.getDebitsPosted());
        assertEquals((long) 0, accounts.getDebitsPending());
        assertEquals((long) 0, accounts.getCreditsPosted());
        assertEquals((long) 0, accounts.getCreditsPending());
        assertEquals((long) 0, accounts.getTimestamp());
    }

    @Test
    public void testId() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setId(100, 200);
        assertEquals(100L, accounts.getId(UInt128.LeastSignificant));
        assertEquals(200L, accounts.getId(UInt128.MostSignificant));
    }

    @Test
    public void testIdAsBytes() {
        var accounts = new AccountBatch(1);
        accounts.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        accounts.setId(id);
        assertArrayEquals(id, accounts.getId());
    }

    public void testIdNull() {
        byte[] id = null;
        var accounts = new AccountBatch(1);

        accounts.add();
        accounts.setId(id);

        assertArrayEquals(new byte[16], accounts.getId());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIdInvalid() {
        var accounts = new AccountBatch(1);
        accounts.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        accounts.setId(id);
        assert false;
    }

    @Test
    public void testUserData() {
        var accounts = new AccountBatch(2);
        accounts.add();

        accounts.setUserData(100, 200);
        assertEquals(100L, accounts.getUserData(UInt128.LeastSignificant));
        assertEquals(200L, accounts.getUserData(UInt128.MostSignificant));
    }

    @Test
    public void testUserDataAsBytes() {
        var accounts = new AccountBatch(1);
        accounts.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        accounts.setUserData(id);
        assertArrayEquals(id, accounts.getUserData());
    }

    @Test
    public void testUserDataNull() {
        var accounts = new AccountBatch(1);
        accounts.add();

        byte[] userData = null;
        accounts.setUserData(userData);
        assertEquals(0L, accounts.getUserData(UInt128.LeastSignificant));
        assertEquals(0L, accounts.getUserData(UInt128.MostSignificant));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserDataInvalid() {
        var accounts = new AccountBatch(1);
        accounts.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        accounts.setUserData(id);
        assert false;
    }

    @Test
    public void testLedger() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setLedger(200);
        assertEquals(200, accounts.getLedger());
    }

    @Test
    public void testCode() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setCode(30);
        assertEquals(30, accounts.getCode());
    }

    @Test
    public void testCodeUnsignedValue() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setCode(60000);
        assertEquals(60000, accounts.getCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCodeNegative() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setCode(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCodeOverflow() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setCode(Integer.MAX_VALUE);
    }

    @Test
    public void testReserved() {
        var accounts = new AccountBatch(1);
        accounts.add();

        var reserved = new byte[48];
        reserved[0] = 100;
        reserved[47] = 101;
        accounts.setReserved(reserved);
        assertArrayEquals(reserved, accounts.getReserved());
    }

    @Test
    public void testReservedNull() {
        var accounts = new AccountBatch(1);
        accounts.add();

        byte[] reserved = null;
        accounts.setReserved(reserved);
        assertArrayEquals(new byte[48], accounts.getReserved());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReservedInvalid() {
        var accounts = new AccountBatch(1);
        accounts.add();

        var reserved = new byte[49];
        accounts.setReserved(reserved);
        assert false;
    }

    @Test
    public void testFlags() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setFlags(AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS | AccountFlags.LINKED);
        assertEquals((int) (AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS | AccountFlags.LINKED),
                accounts.getFlags());
    }

    @Test
    public void testFlagsUnsignedValue() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setFlags(60000);
        assertEquals(60000, accounts.getFlags());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlagsNegative() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setFlags(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlagsOverflow() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setFlags(Integer.MAX_VALUE);
    }

    @Test
    public void testCreditsPending() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setCreditsPending(999);
        assertEquals((long) 999, accounts.getCreditsPending());
    }

    @Test
    public void testCreditsPosted() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setCreditsPosted(999);
        assertEquals((long) 999, accounts.getCreditsPosted());
    }

    @Test
    public void testDebitsPosted() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setDebitsPosted(999);
        assertEquals((long) 999, accounts.getDebitsPosted());
    }

    @Test
    public void testDebitsPending() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setDebitsPending(999);
        assertEquals((long) 999, accounts.getDebitsPending());
    }

    @Test
    public void testTimestamp() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setTimestamp(1234567890);
        assertEquals((long) 1234567890, accounts.getTimestamp());
    }
}
