package com.tigerbeetle;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.junit.Test;

public class CreateAndReturnTransferResultTest {
    @Test
    public void testDefaultValues() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        assertEquals(CreateTransferResult.Ok, results.getResult());
        assertEquals(0L, results.getTimestamp());
        assertEquals(BigInteger.ZERO, results.getAmount());
        assertEquals(0L, results.getAmount(UInt128.LeastSignificant));
        assertEquals(0L, results.getAmount(UInt128.MostSignificant));
        assertEquals(0L, results.getFlags());
        assertEquals(BigInteger.ZERO, results.getDebitAccountDebitsPending());
        assertEquals(0L, results.getDebitAccountDebitsPending(UInt128.LeastSignificant));
        assertEquals(0L, results.getDebitAccountDebitsPending(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, results.getDebitAccountDebitsPosted());
        assertEquals(0L, results.getDebitAccountDebitsPosted(UInt128.LeastSignificant));
        assertEquals(0L, results.getDebitAccountDebitsPosted(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, results.getDebitAccountCreditsPending());
        assertEquals(0L, results.getDebitAccountCreditsPending(UInt128.LeastSignificant));
        assertEquals(0L, results.getDebitAccountCreditsPending(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, results.getDebitAccountCreditsPosted());
        assertEquals(0L, results.getDebitAccountCreditsPosted(UInt128.LeastSignificant));
        assertEquals(0L, results.getDebitAccountCreditsPosted(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, results.getCreditAccountDebitsPending());
        assertEquals(0L, results.getCreditAccountDebitsPending(UInt128.LeastSignificant));
        assertEquals(0L, results.getCreditAccountDebitsPending(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, results.getCreditAccountDebitsPosted());
        assertEquals(0L, results.getCreditAccountDebitsPosted(UInt128.LeastSignificant));
        assertEquals(0L, results.getCreditAccountDebitsPosted(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, results.getCreditAccountCreditsPending());
        assertEquals(0L, results.getCreditAccountCreditsPending(UInt128.LeastSignificant));
        assertEquals(0L, results.getCreditAccountCreditsPending(UInt128.MostSignificant));
        assertEquals(BigInteger.ZERO, results.getCreditAccountCreditsPosted());
        assertEquals(0L, results.getCreditAccountCreditsPosted(UInt128.LeastSignificant));
        assertEquals(0L, results.getCreditAccountCreditsPosted(UInt128.MostSignificant));
    }

    @Test
    public void testResult() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        results.setResult(CreateTransferResult.Exists);
        assertEquals(CreateTransferResult.Exists, results.getResult());
    }

    @Test
    public void testTimestamp() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = Long.MAX_VALUE;
        results.setTimestamp(value);
        assertEquals(value, results.getTimestamp());
    }

    @Test
    public void testFlags() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = Integer.MAX_VALUE;
        results.setFlags(value);
        assertEquals(value, results.getFlags());
    }

    @Test
    public void testAmount() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = new BigInteger("123456789012345678901234567890");
        results.setAmount(value);
        assertEquals(value, results.getAmount());
    }

    @Test
    public void testAmountLong() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        results.setAmount(999);
        assertEquals(BigInteger.valueOf(999), results.getAmount());

        results.setAmount(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), results.getAmount());
        assertEquals(999L, results.getAmount(UInt128.LeastSignificant));
        assertEquals(1L, results.getAmount(UInt128.MostSignificant));
    }

    @Test
    public void testDebitAccountDebitsPending() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = new BigInteger("123456789012345678901234567890");
        results.setDebitAccountDebitsPending(value);
        assertEquals(value, results.getDebitAccountDebitsPending());
    }

    @Test
    public void testDebitAccountDebitsPendingLong() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        results.setDebitAccountDebitsPending(999);
        assertEquals(BigInteger.valueOf(999), results.getDebitAccountDebitsPending());

        results.setDebitAccountDebitsPending(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), results.getDebitAccountDebitsPending());
        assertEquals(999L, results.getDebitAccountDebitsPending(UInt128.LeastSignificant));
        assertEquals(1L, results.getDebitAccountDebitsPending(UInt128.MostSignificant));
    }

    @Test
    public void testDebitAccountDebitsPosted() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = new BigInteger("123456789012345678901234567890");
        results.setDebitAccountDebitsPosted(value);
        assertEquals(value, results.getDebitAccountDebitsPosted());
    }

    @Test
    public void testDebitAccountDebitsPostedLong() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        results.setDebitAccountDebitsPosted(999);
        assertEquals(BigInteger.valueOf(999), results.getDebitAccountDebitsPosted());

        results.setDebitAccountDebitsPosted(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), results.getDebitAccountDebitsPosted());
        assertEquals(999L, results.getDebitAccountDebitsPosted(UInt128.LeastSignificant));
        assertEquals(1L, results.getDebitAccountDebitsPosted(UInt128.MostSignificant));
    }

    @Test
    public void testDebitAccountCreditsPending() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = new BigInteger("123456789012345678901234567890");
        results.setDebitAccountCreditsPending(value);
        assertEquals(value, results.getDebitAccountCreditsPending());
    }

    @Test
    public void testDebitAccountCreditsPendingLong() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        results.setDebitAccountCreditsPending(999);
        assertEquals(BigInteger.valueOf(999), results.getDebitAccountCreditsPending());

        results.setDebitAccountCreditsPending(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), results.getDebitAccountCreditsPending());
        assertEquals(999L, results.getDebitAccountCreditsPending(UInt128.LeastSignificant));
        assertEquals(1L, results.getDebitAccountCreditsPending(UInt128.MostSignificant));
    }

    @Test
    public void testDebitAccountCreditsPosted() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = new BigInteger("123456789012345678901234567890");
        results.setDebitAccountCreditsPosted(value);
        assertEquals(value, results.getDebitAccountCreditsPosted());
    }

    @Test
    public void testDebitAccountCreditsPostedLong() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        results.setDebitAccountCreditsPosted(999);
        assertEquals(BigInteger.valueOf(999), results.getDebitAccountCreditsPosted());

        results.setDebitAccountCreditsPosted(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), results.getDebitAccountCreditsPosted());
        assertEquals(999L, results.getDebitAccountCreditsPosted(UInt128.LeastSignificant));
        assertEquals(1L, results.getDebitAccountCreditsPosted(UInt128.MostSignificant));
    }

    @Test
    public void testCreditAccountDebitsPending() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = new BigInteger("123456789012345678901234567890");
        results.setCreditAccountDebitsPending(value);
        assertEquals(value, results.getCreditAccountDebitsPending());
    }

    @Test
    public void testCreditAccountDebitsPendingLong() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        results.setCreditAccountDebitsPending(999);
        assertEquals(BigInteger.valueOf(999), results.getCreditAccountDebitsPending());

        results.setCreditAccountDebitsPending(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), results.getCreditAccountDebitsPending());
        assertEquals(999L, results.getCreditAccountDebitsPending(UInt128.LeastSignificant));
        assertEquals(1L, results.getCreditAccountDebitsPending(UInt128.MostSignificant));
    }

    @Test
    public void testCreditAccountDebitsPosted() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = new BigInteger("123456789012345678901234567890");
        results.setCreditAccountDebitsPosted(value);
        assertEquals(value, results.getCreditAccountDebitsPosted());
    }

    @Test
    public void testCreditAccountDebitsPostedLong() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        results.setCreditAccountDebitsPosted(999);
        assertEquals(BigInteger.valueOf(999), results.getCreditAccountDebitsPosted());

        results.setCreditAccountDebitsPosted(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), results.getCreditAccountDebitsPosted());
        assertEquals(999L, results.getCreditAccountDebitsPosted(UInt128.LeastSignificant));
        assertEquals(1L, results.getCreditAccountDebitsPosted(UInt128.MostSignificant));
    }

    @Test
    public void testCreditAccountCreditsPending() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = new BigInteger("123456789012345678901234567890");
        results.setCreditAccountCreditsPending(value);
        assertEquals(value, results.getCreditAccountCreditsPending());
    }

    @Test
    public void testCreditAccountCreditsPendingLong() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        results.setCreditAccountCreditsPending(999);
        assertEquals(BigInteger.valueOf(999), results.getCreditAccountCreditsPending());

        results.setCreditAccountCreditsPending(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), results.getCreditAccountCreditsPending());
        assertEquals(999L, results.getCreditAccountCreditsPending(UInt128.LeastSignificant));
        assertEquals(1L, results.getCreditAccountCreditsPending(UInt128.MostSignificant));
    }

    @Test
    public void testCreditAccountCreditsPosted() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        final var value = new BigInteger("123456789012345678901234567890");
        results.setCreditAccountCreditsPosted(value);
        assertEquals(value, results.getCreditAccountCreditsPosted());
    }

    @Test
    public void testCreditAccountCreditsPostedLong() {
        final var results = new CreateAndReturnTransferResultBatch(1);
        results.add();

        results.setCreditAccountCreditsPosted(999);
        assertEquals(BigInteger.valueOf(999), results.getCreditAccountCreditsPosted());

        results.setCreditAccountCreditsPosted(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), results.getCreditAccountCreditsPosted());
        assertEquals(999L, results.getCreditAccountCreditsPosted(UInt128.LeastSignificant));
        assertEquals(1L, results.getCreditAccountCreditsPosted(UInt128.MostSignificant));
    }
}
