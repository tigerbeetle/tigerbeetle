package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.junit.Test;

public class AccountTest {

    @Test
    public void testDefaultValues() {
        var accounts = new AccountBatch(1);
        accounts.add();

        assertEquals(0L, accounts.getId(UInt128.LeastSignificant));
        assertEquals(0L, accounts.getId(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, accounts.getDebitsPosted());
        assertEquals(BigInteger.ZERO, accounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, accounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, accounts.getCreditsPending());
        assertEquals(0L, accounts.getUserData128(UInt128.LeastSignificant));
        assertEquals(0L, accounts.getUserData128(UInt128.MostSignificant));
        assertEquals(0L, accounts.getUserData64());
        assertEquals(0, accounts.getUserData32());
        assertEquals(0, accounts.getLedger());
        assertEquals(AccountFlags.NONE, accounts.getFlags());
        assertEquals(0L, accounts.getTimestamp());
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
    public void testIdLong() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setId(100);
        assertEquals(100L, accounts.getId(UInt128.LeastSignificant));
        assertEquals(0L, accounts.getId(UInt128.MostSignificant));
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
    public void testCreditsPending() {
        var accounts = new AccountBatch(1);
        accounts.add();

        final var value = new BigInteger("123456789012345678901234567890");
        accounts.setCreditsPending(value);
        assertEquals(value, accounts.getCreditsPending());
    }


    @Test
    public void testCreditsPendingLong() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setCreditsPending(999);
        assertEquals(BigInteger.valueOf(999), accounts.getCreditsPending());

        accounts.setCreditsPending(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), accounts.getCreditsPending());
        assertEquals(999L, accounts.getCreditsPending(UInt128.LeastSignificant));
        assertEquals(1L, accounts.getCreditsPending(UInt128.MostSignificant));
    }

    @Test
    public void testCreditsPosted() {
        var accounts = new AccountBatch(1);
        accounts.add();

        final var value = new BigInteger("123456789012345678901234567890");
        accounts.setCreditsPosted(value);
        assertEquals(value, accounts.getCreditsPosted());
    }

    @Test
    public void testCreditsPostedLong() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setCreditsPosted(999);
        assertEquals(BigInteger.valueOf(999), accounts.getCreditsPosted());

        accounts.setCreditsPosted(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), accounts.getCreditsPosted());
        assertEquals(999L, accounts.getCreditsPosted(UInt128.LeastSignificant));
        assertEquals(1L, accounts.getCreditsPosted(UInt128.MostSignificant));
    }

    @Test
    public void testDebitsPosted() {
        var accounts = new AccountBatch(1);
        accounts.add();

        final var value = new BigInteger("123456789012345678901234567890");
        accounts.setDebitsPosted(value);
        assertEquals(value, accounts.getDebitsPosted());
    }

    @Test
    public void testDebitsPostedLong() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setDebitsPosted(999);
        assertEquals(BigInteger.valueOf(999), accounts.getDebitsPosted());

        accounts.setDebitsPosted(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), accounts.getDebitsPosted());
        assertEquals(999L, accounts.getDebitsPosted(UInt128.LeastSignificant));
        assertEquals(1L, accounts.getDebitsPosted(UInt128.MostSignificant));
    }

    @Test
    public void testDebitsPending() {
        var accounts = new AccountBatch(1);
        accounts.add();

        final var value = new BigInteger("123456789012345678901234567890");
        accounts.setDebitsPending(value);
        assertEquals(value, accounts.getDebitsPending());
    }

    @Test
    public void testDebitsPendingLong() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setDebitsPending(999);
        assertEquals(BigInteger.valueOf(999), accounts.getDebitsPending());

        accounts.setDebitsPending(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), accounts.getDebitsPending());
        assertEquals(999L, accounts.getDebitsPending(UInt128.LeastSignificant));
        assertEquals(1L, accounts.getDebitsPending(UInt128.MostSignificant));
    }

    @Test
    public void testUserData128Long() {
        var accounts = new AccountBatch(2);
        accounts.add();

        accounts.setUserData128(100);
        assertEquals(100L, accounts.getUserData128(UInt128.LeastSignificant));
        assertEquals(0L, accounts.getUserData128(UInt128.MostSignificant));
    }

    @Test
    public void testUserData128() {
        var accounts = new AccountBatch(2);
        accounts.add();

        accounts.setUserData128(100, 200);
        assertEquals(100L, accounts.getUserData128(UInt128.LeastSignificant));
        assertEquals(200L, accounts.getUserData128(UInt128.MostSignificant));
    }

    @Test
    public void testUserData128AsBytes() {
        var accounts = new AccountBatch(1);
        accounts.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        accounts.setUserData128(id);
        assertArrayEquals(id, accounts.getUserData128());
    }

    @Test
    public void testUserData128Null() {
        var accounts = new AccountBatch(1);
        accounts.add();

        byte[] userData = null;
        accounts.setUserData128(userData);
        assertEquals(0L, accounts.getUserData128(UInt128.LeastSignificant));
        assertEquals(0L, accounts.getUserData128(UInt128.MostSignificant));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserData128Invalid() {
        var accounts = new AccountBatch(1);
        accounts.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        accounts.setUserData128(id);
        assert false;
    }

    @Test
    public void testUserData64() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setUserData64(1000L);
        assertEquals(1000L, accounts.getUserData64());
    }

    @Test
    public void testUserData32() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setUserData32(100);
        assertEquals(100, accounts.getUserData32());
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

        accounts.setReserved(0);
        assertEquals(0, accounts.getReserved());
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
    public void testTimestamp() {
        var accounts = new AccountBatch(1);
        accounts.add();

        accounts.setTimestamp(1234567890);
        assertEquals((long) 1234567890, accounts.getTimestamp());
    }
}
