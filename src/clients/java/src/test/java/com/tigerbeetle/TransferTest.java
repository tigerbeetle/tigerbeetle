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
        assertEquals(BigInteger.ZERO, transfers.getAmount());
        assertEquals(0L, transfers.getPendingId(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getPendingId(UInt128.MostSignificant));
        assertEquals(0L, transfers.getUserData128(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getUserData128(UInt128.MostSignificant));
        assertEquals(0L, transfers.getUserData64());
        assertEquals(0, transfers.getUserData32());
        assertEquals(0, transfers.getTimeout());
        assertEquals(0, transfers.getLedger());
        assertEquals(0, transfers.getCode());
        assertEquals(TransferFlags.NONE, transfers.getFlags());
        assertEquals(0L, transfers.getTimestamp());
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
    public void testIdLong() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setId(100);
        assertEquals(100L, transfers.getId(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getId(UInt128.MostSignificant));
    }

    @Test
    public void testIdAsBytes() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        transfers.setId(id);
        assertArrayEquals(id, transfers.getId());
    }

    @Test
    public void testIdNull() {
        var transfers = new TransferBatch(1);
        transfers.add();

        byte[] id = null;
        transfers.setId(id);

        assertArrayEquals(new byte[16], transfers.getId());
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
    public void testDebitAccountIdLong() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setDebitAccountId(100);
        assertEquals(100L, transfers.getDebitAccountId(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getDebitAccountId(UInt128.MostSignificant));
    }

    @Test
    public void testDebitAccountIdAsBytes() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        transfers.setDebitAccountId(id);
        assertArrayEquals(id, transfers.getDebitAccountId());
    }

    @Test
    public void testDebitAccountIdNull() {
        var transfers = new TransferBatch(1);
        transfers.add();

        byte[] debitAccountId = null;
        transfers.setDebitAccountId(debitAccountId);

        assertArrayEquals(new byte[16], transfers.getDebitAccountId());
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
    public void testCreditAccountIdLong() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setCreditAccountId(100);
        assertEquals(100L, transfers.getCreditAccountId(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getCreditAccountId(UInt128.MostSignificant));
    }

    @Test
    public void testCreditAccountIdAsBytes() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        transfers.setCreditAccountId(id);
        assertArrayEquals(id, transfers.getCreditAccountId());
    }

    @Test
    public void testCreditAccountIdNull() {
        var transfers = new TransferBatch(1);
        transfers.add();

        byte[] creditAccountId = null;
        transfers.setCreditAccountId(creditAccountId);

        assertArrayEquals(new byte[16], transfers.getCreditAccountId());
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
    public void testAmount() {
        var transfers = new TransferBatch(1);
        transfers.add();

        final var value = new BigInteger("123456789012345678901234567890");
        transfers.setAmount(value);
        assertEquals(value, transfers.getAmount());
    }

    @Test
    public void testAmountLong() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setAmount(999);
        assertEquals(BigInteger.valueOf(999), transfers.getAmount());

        transfers.setAmount(999, 1);
        assertEquals(UInt128.asBigInteger(999, 1), transfers.getAmount());
        assertEquals(999L, transfers.getAmount(UInt128.LeastSignificant));
        assertEquals(1L, transfers.getAmount(UInt128.MostSignificant));
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
    public void testPendingIdLong() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setPendingId(100);
        assertEquals(100L, transfers.getPendingId(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getPendingId(UInt128.MostSignificant));
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
    public void testUserData128Long() {
        var transfers = new TransferBatch(2);
        transfers.add();

        transfers.setUserData128(100);
        assertEquals(100L, transfers.getUserData128(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getUserData128(UInt128.MostSignificant));
    }

    @Test
    public void testUserData128() {
        var transfers = new TransferBatch(2);
        transfers.add();

        transfers.setUserData128(100, 200);
        assertEquals(100L, transfers.getUserData128(UInt128.LeastSignificant));
        assertEquals(200L, transfers.getUserData128(UInt128.MostSignificant));
    }

    @Test
    public void testUserData128AsBytes() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        transfers.setUserData128(id);
        assertArrayEquals(id, transfers.getUserData128());
    }

    @Test
    public void testUserData128Null() {
        var transfers = new TransferBatch(1);
        transfers.add();

        byte[] userData = null;
        transfers.setUserData128(userData);
        assertEquals(0L, transfers.getUserData128(UInt128.LeastSignificant));
        assertEquals(0L, transfers.getUserData128(UInt128.MostSignificant));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserData128Invalid() {
        var transfers = new TransferBatch(1);
        transfers.add();

        var id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        transfers.setUserData128(id);
        assert false;
    }

    @Test
    public void testUserData64() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setUserData64(1000L);
        assertEquals(1000L, transfers.getUserData64());
    }

    @Test
    public void testUserData32() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setUserData32(100);
        assertEquals(100, transfers.getUserData32());
    }

    @Test
    public void testTimeout() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setTimeout(9999);
        assertEquals(9999, transfers.getTimeout());
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
    public void testTimestamp() {
        var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setTimestamp(1234567890);
        assertEquals((long) 1234567890, transfers.getTimestamp());
    }
}
