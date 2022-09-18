package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import java.math.BigInteger;
import java.util.UUID;
import org.junit.Test;

public class TransferTest {

    @Test
    public void testDefaultValues() {
        var transfer = new Transfer();
        assertEquals(new UUID(0, 0), transfer.getId());
        assertEquals(new UUID(0, 0), transfer.getDebitAccountId());
        assertEquals(new UUID(0, 0), transfer.getCreditAccountId());
        assertEquals(new UUID(0, 0), transfer.getUserData());
        assertEquals(new UUID(0, 0), transfer.getPendingId());
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
        transfer.setId(new UUID(100, 200));
        assertEquals(new UUID(100, 200), transfer.getId());
    }

    @Test(expected = NullPointerException.class)
    public void testIdNull() {
        var transfer = new Transfer();
        transfer.setId(null);
    }

    @Test
    public void testDebitAccountId() {
        var transfer = new Transfer();
        transfer.setDebitAccountId(new UUID(100, 200));
        assertEquals(new UUID(100, 200), transfer.getDebitAccountId());
    }

    @Test(expected = NullPointerException.class)
    public void testDebitAccountIdNull() {
        var transfer = new Transfer();
        transfer.setDebitAccountId(null);
    }

    @Test
    public void testCreditAccountId() {
        var transfer = new Transfer();
        transfer.setCreditAccountId(new UUID(100, 200));
        assertEquals(new UUID(100, 200), transfer.getCreditAccountId());
    }

    @Test(expected = NullPointerException.class)
    public void testCreditAccountIdNull() {
        var transfer = new Transfer();
        transfer.setCreditAccountId(null);
    }

    @Test
    public void testUserData() {
        var transfer = new Transfer();
        transfer.setUserData(new UUID(100, 200));
        assertEquals(new UUID(100, 200), transfer.getUserData());
    }

    @Test
    public void testUserDataNull() {
        var transfer = new Transfer();
        transfer.setUserData(null);
        assertEquals(new UUID(0, 0), transfer.getUserData());
    }

    @Test
    public void testPendingId() {
        var transfer = new Transfer();
        transfer.setPendingId(new UUID(100, 200));
        assertEquals(new UUID(100, 200), transfer.getPendingId());
    }

    @Test
    public void testPendingIdNull() {
        var transfer = new Transfer();
        transfer.setPendingId(null);
        assertEquals(new UUID(0, 0), transfer.getPendingId());
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
