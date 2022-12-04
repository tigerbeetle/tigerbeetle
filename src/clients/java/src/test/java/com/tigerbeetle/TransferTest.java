package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.math.BigInteger;
import org.junit.Test;

public class TransferTest {

    @Test
    public void testDefaultValues() {
        var transfers = new TransferBatch(1);
        transfers.add();
        assertEquals(0L, transfers.getId(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getId(UInt128.MostSignificant));
        assertEquals(0L, transfers.getDebitAccountId(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getDebitAccountId(UInt128.MostSignificant));
        assertEquals(0L, transfers.getCreditAccountId(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getCreditAccountId(UInt128.MostSignificant));
        assertEquals(0L, transfers.getUserData(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getUserData(UInt128.MostSignificant));
        assertEquals(0L, transfers.getPendingId(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getPendingId(UInt128.MostSignificant));
        assertEquals((long) 0, transfers.getTimeout());
        assertEquals(0, transfers.getLedger());
        assertEquals(0, transfers.getCode());
        assertEquals((int) TransferFlags.NONE, transfers.getFlags());
        assertEquals((long) 0, transfers.getAmount());
        assertEquals((long) 0, transfers.getTimestamp());
    }

    @Test
    public void testId() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setId(100, 200);
        assertEquals(100L, transfers.getId(UInt128.LeastSignificant));
        assertEquals(200L, transfers.getId(UInt128.MostSignificant));
    }

    @Test
    public void testIdAsBytes() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        transfers.setId(id);
        assertArrayEquals(id, transfers.getId());
    }

    @Test(expected = NullPointerException.class)
    public void testIdNull() {
        var transfers = new TransferBatch(1);
        transfers.add();

        byte[] id = null;
        transfers.setId(id);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIdInvalid() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        transfers.setId(id);
        assert false;
    }

    @Test
    public void testDebitAccountId() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setDebitAccountId(100, 200);
        assertEquals(100L, transfers.getDebitAccountId(UInt128.LeastSignificant));
        assertEquals(200L, transfers.getDebitAccountId(UInt128.MostSignificant));
    }

    @Test
    public void testDebitAccountIdAsBytes() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        transfers.setDebitAccountId(id);
        assertArrayEquals(id, transfers.getDebitAccountId());
    }

    @Test(expected = NullPointerException.class)
    public void testDebitAccountIdNull() {
        var transfers = new TransferBatch(1);
        transfers.add();

        byte[] debitAccountId = null;
        transfers.setDebitAccountId(debitAccountId);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDebitAccountIdInvalid() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        transfers.setDebitAccountId(id);
        assert false;
    }

    @Test
    public void testCreditAccountId() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setCreditAccountId(100, 200);
        assertEquals(100L, transfers.getCreditAccountId(UInt128.LeastSignificant));
        assertEquals(200L, transfers.getCreditAccountId(UInt128.MostSignificant));
    }

    @Test
    public void testCreditAccountIdAsBytes() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        transfers.setCreditAccountId(id);
        assertArrayEquals(id, transfers.getCreditAccountId());
    }

    @Test(expected = NullPointerException.class)
    public void testCreditAccountIdNull() {
        var transfers = new TransferBatch(1);
        transfers.add();

        byte[] creditAccountId = null;
        transfers.setCreditAccountId(creditAccountId);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreditAccountIdInvalid() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        transfers.setCreditAccountId(id);
        assert false;
    }

    @Test
    public void testUserData() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setUserData(100, 200);
        assertEquals(100L, transfers.getUserData(UInt128.LeastSignificant));
        assertEquals(200L, transfers.getUserData(UInt128.MostSignificant));
    }

    @Test
    public void testUserDataAsBytes() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var userData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        transfers.setUserData(userData);
        assertArrayEquals(userData, transfers.getUserData());
    }

    @Test
    public void testUserDataNull() {
        var transfers = new TransferBatch(1);
        transfers.add();

        byte[] userData = null;
        transfers.setUserData(userData);
        assertEquals(0L, transfers.getUserData(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getUserData(UInt128.MostSignificant));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserDataInvalid() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        transfers.setUserData(id);
        assert false;
    }

    @Test
    public void testPendingId() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setPendingId(100, 200);
        assertEquals(100L, transfers.getPendingId(UInt128.LeastSignificant));
        assertEquals(200L, transfers.getPendingId(UInt128.MostSignificant));
    }

    @Test
    public void testPendingIdAsBytes() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        transfers.setPendingId(id);
        assertArrayEquals(id, transfers.getPendingId());
    }

    @Test
    public void testPendingIdNull() {
        var transfers = new TransferBatch(1);
        transfers.add();

        byte[] pendingId = null;
        transfers.setPendingId(pendingId);
        assertEquals(0L, transfers.getPendingId(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getPendingId(UInt128.MostSignificant));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPendingIdInvalid() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        transfers.setPendingId(id);
        assert false;
    }

    @Test
    public void testTimeout() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setTimeout(9999);
        assertEquals((long) 9999, transfers.getTimeout());
    }

    @Test
    public void testLedger() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setLedger(200);
        assertEquals(200, transfers.getLedger());
    }

    @Test
    public void testCode() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setCode(30);
        assertEquals(30, transfers.getCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCodeNegative() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setCode(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCodeOverflow() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setCode(Integer.MAX_VALUE);
    }

    @Test
    public void testCodeUnsigned() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setCode(53000);
        assertEquals(53000, transfers.getCode());
    }

    @Test
    public void testFlags() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setFlags(TransferFlags.POST_PENDING_TRANSFER | TransferFlags.LINKED);
        assertEquals((int) (TransferFlags.POST_PENDING_TRANSFER | TransferFlags.LINKED),
                transfers.getFlags());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlagsNegative() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setFlags(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlagsOverflow() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setFlags(Integer.MAX_VALUE);
    }

    @Test
    public void testFlagsUnsigned() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setFlags(60000);
        assertEquals(60000, transfers.getFlags());
    }

    @Test
    public void testAmount() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setAmount(999);
        assertEquals((long) 999, transfers.getAmount());
    }

    @Test
    public void testAmountOverflow() {

        // Java long is always signed
        // Larger values must be reinterpreted as signed BigIntegers

        final BigInteger UNSIGNED_LONG_MASK =
                BigInteger.ONE.shiftLeft(Long.SIZE).subtract(BigInteger.ONE);
        BigInteger largeAmount = BigInteger.valueOf(Long.MAX_VALUE + 999).and(UNSIGNED_LONG_MASK);

        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setAmount(largeAmount.longValue());
        assertEquals(largeAmount,
                BigInteger.valueOf(transfers.getAmount()).and(UNSIGNED_LONG_MASK));
    }

    @Test
    public void testTimestamp() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setTimestamp(1234567890);
        assertEquals((long) 1234567890, transfers.getTimestamp());
    }

}
