package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import java.math.BigInteger;
import org.junit.Test;
import com.tigerbeetle.UInt128.Bytes;

public class TransferTest {

    @Test
    public void testDefaultValues() {
        var transfer = new Transfer();
        assertEquals(0L, transfer.getId(Bytes.LeastSignificant));
        assertEquals(0L, transfer.getId(Bytes.MostSignificant));
        assertEquals(0L, transfer.getDebitAccountId(Bytes.LeastSignificant));
        assertEquals(0L, transfer.getDebitAccountId(Bytes.MostSignificant));
        assertEquals(0L, transfer.getCreditAccountId(Bytes.LeastSignificant));
        assertEquals(0L, transfer.getCreditAccountId(Bytes.MostSignificant));
        assertEquals(0L, transfer.getUserData(Bytes.LeastSignificant));
        assertEquals(0L, transfer.getUserData(Bytes.MostSignificant));
        assertEquals(0L, transfer.getPendingId(Bytes.LeastSignificant));
        assertEquals(0L, transfer.getPendingId(Bytes.MostSignificant));
        assertEquals((long) 0, transfer.getTimeout());
        assertEquals(0, transfer.getLedger());
        assertEquals(0, transfer.getCode());
        assertEquals((int) TransferFlags.NONE, transfer.getFlags());
        assertEquals((long) 0, transfer.getAmount());
        assertEquals((long) 0, transfer.getTimestamp());
    }

    @Test
    public void testId() {
        var transfer = new Transfer();
        transfer.setId(100, 200);
        assertEquals(100L, transfer.getId(Bytes.LeastSignificant));
        assertEquals(200L, transfer.getId(Bytes.MostSignificant));
    }

    @Test(expected = NullPointerException.class)
    public void testIdNull() {
        byte[] id = null;
        var transfer = new Transfer();
        transfer.setId(id);
    }

    @Test
    public void testDebitAccountId() {
        var transfer = new Transfer();
        transfer.setDebitAccountId(100, 200);
        assertEquals(100L, transfer.getDebitAccountId(Bytes.LeastSignificant));
        assertEquals(200L, transfer.getDebitAccountId(Bytes.MostSignificant));
    }

    @Test(expected = NullPointerException.class)
    public void testDebitAccountIdNull() {
        byte[] debitAccountId = null;
        var transfer = new Transfer();
        transfer.setDebitAccountId(debitAccountId);
    }

    @Test
    public void testCreditAccountId() {
        var transfer = new Transfer();
        transfer.setCreditAccountId(100, 200);
        assertEquals(100L, transfer.getCreditAccountId(Bytes.LeastSignificant));
        assertEquals(200L, transfer.getCreditAccountId(Bytes.MostSignificant));
    }

    @Test(expected = NullPointerException.class)
    public void testCreditAccountIdNull() {
        byte[] creditAccountId = null;
        var transfer = new Transfer();
        transfer.setCreditAccountId(creditAccountId);
    }

    @Test
    public void testUserData() {
        var transfer = new Transfer();
        transfer.setUserData(100, 200);
        assertEquals(100L, transfer.getUserData(Bytes.LeastSignificant));
        assertEquals(200L, transfer.getUserData(Bytes.MostSignificant));
    }

    @Test
    public void testUserDataNull() {
        byte[] userData = null;
        var transfer = new Transfer();
        transfer.setUserData(userData);
        assertEquals(0L, transfer.getUserData(Bytes.LeastSignificant));
        assertEquals(0L, transfer.getUserData(Bytes.MostSignificant));
    }

    @Test
    public void testPendingId() {
        var transfer = new Transfer();
        transfer.setPendingId(100, 200);
        assertEquals(100L, transfer.getPendingId(Bytes.LeastSignificant));
        assertEquals(200L, transfer.getPendingId(Bytes.MostSignificant));
    }

    @Test
    public void testPendingIdNull() {
        byte[] pendingId = null;
        var transfer = new Transfer();
        transfer.setPendingId(pendingId);
        assertEquals(0L, transfer.getPendingId(Bytes.LeastSignificant));
        assertEquals(0L, transfer.getPendingId(Bytes.MostSignificant));
    }

    @Test
    public void testTimeout() {
        var transfer = new Transfer();
        transfer.setTimeout(9999);
        assertEquals((long) 9999, transfer.getTimeout());
    }

    @Test
    public void testLedger() {
        var transfer = new Transfer();
        transfer.setLedger(200);
        assertEquals(200, transfer.getLedger());
    }

    @Test
    public void testCode() {
        var transfer = new Transfer();
        transfer.setCode(30);
        assertEquals(30, transfer.getCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCodeNegative() {
        var transfer = new Transfer();
        transfer.setCode(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCodeOverflow() {
        var transfer = new Transfer();
        transfer.setCode(Integer.MAX_VALUE);
    }

    @Test
    public void testFlags() {
        var transfer = new Transfer();
        transfer.setFlags(TransferFlags.POST_PENDING_TRANSFER | TransferFlags.LINKED);
        assertEquals((int) (TransferFlags.POST_PENDING_TRANSFER | TransferFlags.LINKED),
                transfer.getFlags());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlagsNegative() {
        var transfer = new Transfer();
        transfer.setFlags(-1);
    }

    @Test
    public void testAmount() {
        var transfer = new Transfer();
        transfer.setAmount(999);
        assertEquals((long) 999, transfer.getAmount());
    }

    @Test
    public void testAmountOverlflow() {

        // Java long is always signed
        // Larger values must be reinterpreted as signed BigIntegers

        final BigInteger UNSIGNED_LONG_MASK =
                BigInteger.ONE.shiftLeft(Long.SIZE).subtract(BigInteger.ONE);
        BigInteger largeAmount = BigInteger.valueOf(Long.MAX_VALUE + 999).and(UNSIGNED_LONG_MASK);

        var transfer = new Transfer();
        transfer.setAmount(largeAmount.longValue());
        assertEquals(largeAmount, BigInteger.valueOf(transfer.getAmount()).and(UNSIGNED_LONG_MASK));
    }

    @Test
    public void testTimestamp() {
        var transfer = new Transfer();
        transfer.setTimestamp(1234567890);
        assertEquals((long) 1234567890, transfer.getTimestamp());
    }

}
