package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.junit.Test;

public class AccountBalanceTest {

    @Test
    public void testDefaultValues() {
        final var balances = new AccountBalanceBatch(1);
        balances.add();

        assertEquals(0L, balances.getTimestamp());
        assertEquals(BigInteger.ZERO, balances.getDebitsPending());
        assertEquals(0L, balances.getDebitsPending(UInt128.LeastSignificant));
        assertEquals(0L, balances.getDebitsPending(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, balances.getDebitsPosted());
        assertEquals(0L, balances.getDebitsPosted(UInt128.LeastSignificant));
        assertEquals(0L, balances.getDebitsPosted(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, balances.getCreditsPending());
        assertEquals(0L, balances.getCreditsPending(UInt128.LeastSignificant));
        assertEquals(0L, balances.getCreditsPending(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, balances.getCreditsPosted());
        assertEquals(0L, balances.getCreditsPosted(UInt128.LeastSignificant));
        assertEquals(0L, balances.getCreditsPosted(UInt128.MostSignificant));
        assertArrayEquals(new byte[56], balances.getReserved());
    }

    @Test
    public void testCreditsPending() {
        final var balances = new AccountBalanceBatch(1);
        balances.add();

        final var value = new BigInteger("123456789012345678901234567890");
        balances.setCreditsPending(value);
        assertEquals(value, balances.getCreditsPending());
    }


    @Test
    public void testCreditsPendingLong() {
        final var balances = new AccountBalanceBatch(1);
        balances.add();

        balances.setCreditsPending(999);
        assertEquals(BigInteger.valueOf(999), balances.getCreditsPending());

        balances.setCreditsPending(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), balances.getCreditsPending());
        assertEquals(999L, balances.getCreditsPending(UInt128.LeastSignificant));
        assertEquals(1L, balances.getCreditsPending(UInt128.MostSignificant));
    }

    @Test
    public void testCreditsPosted() {
        final var balances = new AccountBalanceBatch(1);
        balances.add();

        final var value = new BigInteger("123456789012345678901234567890");
        balances.setCreditsPosted(value);
        assertEquals(value, balances.getCreditsPosted());
    }

    @Test
    public void testCreditsPostedLong() {
        final var balances = new AccountBalanceBatch(1);
        balances.add();

        balances.setCreditsPosted(999);
        assertEquals(BigInteger.valueOf(999), balances.getCreditsPosted());

        balances.setCreditsPosted(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), balances.getCreditsPosted());
        assertEquals(999L, balances.getCreditsPosted(UInt128.LeastSignificant));
        assertEquals(1L, balances.getCreditsPosted(UInt128.MostSignificant));
    }

    @Test
    public void testDebitsPosted() {
        final var balances = new AccountBalanceBatch(1);
        balances.add();

        final var value = new BigInteger("123456789012345678901234567890");
        balances.setDebitsPosted(value);
        assertEquals(value, balances.getDebitsPosted());
    }

    @Test
    public void testDebitsPostedLong() {
        final var balances = new AccountBalanceBatch(1);
        balances.add();

        balances.setDebitsPosted(999);
        assertEquals(BigInteger.valueOf(999), balances.getDebitsPosted());

        balances.setDebitsPosted(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), balances.getDebitsPosted());
        assertEquals(999L, balances.getDebitsPosted(UInt128.LeastSignificant));
        assertEquals(1L, balances.getDebitsPosted(UInt128.MostSignificant));
    }

    @Test
    public void testDebitsPending() {
        final var balances = new AccountBalanceBatch(1);
        balances.add();

        final var value = new BigInteger("123456789012345678901234567890");
        balances.setDebitsPending(value);
        assertEquals(value, balances.getDebitsPending());
    }

    @Test
    public void testDebitsPendingLong() {
        var balances = new AccountBalanceBatch(1);
        balances.add();

        balances.setDebitsPending(999);
        assertEquals(BigInteger.valueOf(999), balances.getDebitsPending());

        balances.setDebitsPending(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), balances.getDebitsPending());
        assertEquals(999L, balances.getDebitsPending(UInt128.LeastSignificant));
        assertEquals(1L, balances.getDebitsPending(UInt128.MostSignificant));
    }

    @Test
    public void testReserved() {
        var balances = new AccountBalanceBatch(1);
        balances.add();

        final var bytes = new byte[56];
        for (byte i = 0; i < 56; i++) {
            bytes[i] = i;
        }

        balances.setReserved(bytes);
        assertArrayEquals(bytes, balances.getReserved());
    }

}
