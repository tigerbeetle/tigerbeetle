package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.math.BigInteger;
import org.junit.Test;

/**
 * Asserts the memory interpretation from/to a binary stream.
 */
public class BatchTest {

    private static final DummyAccountDto account1;
    private static final DummyAccountDto account2;
    private static final ByteBuffer dummyAccountsStream;

    private static final DummyTransferDto transfer1;
    private static final DummyTransferDto transfer2;
    private static final ByteBuffer dummyTransfersStream;

    private static final long createAccountTimestamp1;
    private static final long createAccountTimestamp2;
    private static final CreateAccountStatus createAccountResult1;
    private static final CreateAccountStatus createAccountResult2;
    private static final ByteBuffer dummyCreateAccountStatussStream;

    private static final long createTransferTimestamp1;
    private static final long createTransferTimestamp2;
    private static final CreateTransferStatus createTransferResult1;
    private static final CreateTransferStatus createTransferResult2;
    private static final ByteBuffer dummyCreateTransferResultsStream;


    private static final byte[] id1;
    private static final long id1LeastSignificant;
    private static final long id1MostSignificant;
    private static final byte[] id2;
    private static final long id2LeastSignificant;
    private static final long id2MostSignificant;
    private static final ByteBuffer dummyIdsStream;

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeCapacity() {
        new AccountBatch(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullBuffer() {
        ByteBuffer buffer = null;
        new TransferBatch(buffer);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testPositionIndexOutOfBounds() {

        var batch = new AccountBatch(1);
        batch.setPosition(1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testPositionIndexNegative() {

        var batch = new CreateTransferResultBatch(1);
        batch.setPosition(-1);
        assert false; // Should be unreachable
    }

    @Test
    public void testNextFromCapacity() {
        var batch = new AccountBatch(2);

        // Creating from capacity
        // Expected position = -1 and length = 0
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(0 * AccountBatch.Struct.SIZE, batch.getBufferLen());

        assertFalse(batch.isValidPosition());

        // Zero elements, next must return false
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());

        // Calling next multiple times on an EMPTY batch is allowed since the cursor does not move.
        // This allows reusing a single instance of an empty batch,
        // avoiding allocations for the common case (empty batch == success).
        batch.next();
        batch.next();
        assertFalse(batch.isValidPosition());

        // Adding 2 elements

        batch.add();
        assertTrue(batch.isValidPosition());
        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        assertEquals(1 * AccountBatch.Struct.SIZE, batch.getBufferLen());

        batch.add();
        assertTrue(batch.isValidPosition());
        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2 * AccountBatch.Struct.SIZE, batch.getBufferLen());

        // reset to the beginning,
        // Expected position -1
        batch.beforeFirst();
        assertFalse(batch.isValidPosition());
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());

        // Moving
        assertTrue(batch.next());
        assertTrue(batch.isValidPosition());
        assertEquals(0, batch.getPosition());

        assertTrue(batch.next());
        assertEquals(1, batch.getPosition());

        // End of the batch
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());

        // Calling next multiple times must throw an exception, preventing the user to assume that
        // an iterated batch is an empty one.
        try {
            batch.next();
            assert false;
        } catch (IndexOutOfBoundsException exception) {
            assert true;
        }
        assertFalse(batch.isValidPosition());
    }

    @Test
    public void testNextFromBuffer() {
        var batch = new AccountBatch(dummyAccountsStream.position(0));

        // Creating from a existing buffer
        // Expected position = -1 and length = 2
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(dummyAccountsStream.capacity(), batch.getBufferLen());
        assertFalse(batch.isValidPosition());

        // Moving
        assertTrue(batch.next());
        assertTrue(batch.isValidPosition());
        assertEquals(0, batch.getPosition());

        assertTrue(batch.next());
        assertTrue(batch.isValidPosition());
        assertEquals(1, batch.getPosition());

        // End of the batch
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());

        // Calling next multiple times must throw an exception, preventing the user to assume that
        // an iterated batch is an empty one.
        try {
            batch.next();
            assert false;
        } catch (IndexOutOfBoundsException exception) {
            assert true;
        }
        assertFalse(batch.isValidPosition());
    }

    @Test
    public void testNextEmptyBatch() {
        var batch = new TransferBatch(Request.REPLY_EMPTY);

        // Empty batch
        // Expected position = -1 and length = 0
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(0, batch.getCapacity());
        assertEquals(0, batch.getBufferLen());
        assertFalse(batch.isValidPosition());

        // Before the first element
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());

        // Resting an empty batch
        batch.beforeFirst();

        // Still, before the first element
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());

        // Calling next multiple times on an EMPTY batch is allowed since the cursor does not move.
        // This allows reusing a single instance of an empty batch,
        // avoiding allocations for the common case (empty batch == success).
        batch.next();
        batch.next();
        assertFalse(batch.isValidPosition());
    }

    @Test
    public void testAdd() {
        var batch = new AccountBatch(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(0 * AccountBatch.Struct.SIZE, batch.getBufferLen());

        batch.add();

        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(1 * AccountBatch.Struct.SIZE, batch.getBufferLen());

        batch.add();

        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(2 * AccountBatch.Struct.SIZE, batch.getBufferLen());
    }

    @Test(expected = IllegalStateException.class)
    public void testAddReadOnly() {
        var batch = new AccountBatch(dummyAccountsStream.asReadOnlyBuffer().position(0));
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(2 * AccountBatch.Struct.SIZE, batch.getBufferLen());

        batch.add();
    }

    @Test(expected = IllegalStateException.class)
    public void testReadInvalidPosition() {

        AccountBatch batch = new AccountBatch(dummyAccountsStream.position(0));
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());

        assertFalse(batch.isValidPosition());
        batch.getLedger();
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteInvalidPosition() {

        AccountBatch batch = new AccountBatch(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        assertFalse(batch.isValidPosition());
        batch.setCode(100);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testAddExceedCapacity() {
        var batch = new AccountBatch(1);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(1, batch.getCapacity());
        assertEquals(0 * AccountBatch.Struct.SIZE, batch.getBufferLen());

        batch.add();

        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        assertEquals(1 * AccountBatch.Struct.SIZE, batch.getBufferLen());

        batch.add();
    }

    @Test
    public void testReadAccounts() {

        AccountBatch batch = new AccountBatch(dummyAccountsStream.position(0));
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());

        assertTrue(batch.next());
        assertAccounts(account1, batch);

        assertTrue(batch.next());
        assertAccounts(account2, batch);

        assertFalse(batch.next());
    }

    @Test
    public void testWriteAccounts() {

        AccountBatch batch = new AccountBatch(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        batch.add();
        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        setAccount(batch, account1);

        batch.add();
        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());
        setAccount(batch, account2);

        batch.beforeFirst();

        assertTrue(batch.next());
        assertAccounts(account1, batch);

        assertTrue(batch.next());
        assertAccounts(account2, batch);

        assertFalse(batch.next());

        assertBuffer(dummyAccountsStream, batch.getBuffer());
    }

    @Test
    public void testMoveAndSetAccounts() {

        AccountBatch batch = new AccountBatch(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        // Set index 0
        batch.add();
        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        assertEquals(2, batch.getCapacity());
        setAccount(batch, account1);
        assertAccounts(account1, batch);

        // Set index 1 with account1 again
        batch.add();
        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        setAccount(batch, account1);
        assertAccounts(account1, batch);

        // Replace index 0 with account 2
        batch.setPosition(0);
        assertEquals(0, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        setAccount(batch, account2);

        batch.beforeFirst();

        assertTrue(batch.next());
        assertAccounts(account2, batch);

        assertTrue(batch.next());
        assertAccounts(account1, batch);

        assertFalse(batch.next());
    }

    @Test(expected = AssertionError.class)
    public void testInvalidAccountBuffer() {

        // Invalid size
        var invalidBuffer = ByteBuffer.allocate((AccountBatch.Struct.SIZE * 2) - 1)
                .order(ByteOrder.LITTLE_ENDIAN);

        @SuppressWarnings("unused")
        var batch = new AccountBatch(invalidBuffer);
        assert false;
    }

    @Test
    public void testReadTransfers() {

        var batch = new TransferBatch(dummyTransfersStream.position(0));
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());

        assertTrue(batch.next());
        assertTransfers(transfer1, batch);

        assertTrue(batch.next());
        assertTransfers(transfer2, batch);

        assertFalse(batch.next());
    }

    @Test
    public void testWriteTransfers() {

        var batch = new TransferBatch(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        batch.add();
        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        setTransfer(batch, transfer1);

        batch.add();
        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());
        setTransfer(batch, transfer2);

        batch.beforeFirst();

        assertTrue(batch.next());
        assertTransfers(transfer1, batch);

        assertTrue(batch.next());
        assertTransfers(transfer2, batch);

        assertFalse(batch.next());

        assertBuffer(dummyTransfersStream, batch.getBuffer());
    }

    @Test
    public void testMoveAndSetTransfers() {

        var batch = new TransferBatch(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        // Set index 0
        batch.add();
        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        assertEquals(2, batch.getCapacity());
        setTransfer(batch, transfer1);
        assertTransfers(transfer1, batch);

        // Set index 1 with transfer1 again
        batch.add();
        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        setTransfer(batch, transfer1);
        assertTransfers(transfer1, batch);

        // Replace index 0 with account 2
        batch.setPosition(0);
        assertEquals(0, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        setTransfer(batch, transfer2);

        batch.beforeFirst();

        assertTrue(batch.next());
        assertTransfers(transfer2, batch);

        assertTrue(batch.next());
        assertTransfers(transfer1, batch);

        assertFalse(batch.next());
    }

    @Test(expected = AssertionError.class)
    public void testInvalidTransfersBuffer() {

        // Invalid size
        var invalidBuffer = ByteBuffer.allocate((TransferBatch.Struct.SIZE * 2) - 1)
                .order(ByteOrder.LITTLE_ENDIAN);

        @SuppressWarnings("unused")
        var batch = new TransferBatch(invalidBuffer);
        assert false;
    }

    @Test
    public void testReadCreateAccountStatuss() {

        var batch = new CreateAccountResultBatch(dummyCreateAccountStatussStream.position(0));
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());

        assertTrue(batch.next());
        assertEquals(createAccountTimestamp1, batch.getTimestamp());
        assertEquals(createAccountResult1, batch.getStatus());
        assertEquals(0L, batch.getReserved());

        assertTrue(batch.next());
        assertEquals(createAccountTimestamp2, batch.getTimestamp());
        assertEquals(createAccountResult2, batch.getStatus());
        assertEquals(0L, batch.getReserved());

        assertFalse(batch.next());
    }

    @Test
    public void testWriteCreateAccountStatuss() {
        var batch = new CreateAccountResultBatch(1);
        batch.add();

        batch.setTimestamp(createAccountTimestamp1);
        assertEquals(createAccountTimestamp1, batch.getTimestamp());

        batch.setStatus(createAccountResult1);
        assertEquals(createAccountResult1, batch.getStatus());

        batch.setReserved(100);
        assertEquals(100, batch.getReserved());
    }

    @Test(expected = AssertionError.class)
    public void testInvalidCreateAccountStatussBuffer() {

        // Invalid size
        var invalidBuffer = ByteBuffer.allocate((CreateAccountResultBatch.Struct.SIZE * 2) - 1)
                .order(ByteOrder.LITTLE_ENDIAN);

        @SuppressWarnings("unused")
        var batch = new CreateAccountResultBatch(invalidBuffer);
        assert false;
    }

    @Test
    public void testReadCreateTransferStatuss() {

        var batch = new CreateTransferResultBatch(dummyCreateTransferResultsStream.position(0));
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());

        assertTrue(batch.next());
        assertEquals(createTransferTimestamp1, batch.getTimestamp());
        assertEquals(createTransferResult1, batch.getStatus());
        assertEquals(0L, batch.getReserved());

        assertTrue(batch.next());
        assertEquals(createTransferTimestamp2, batch.getTimestamp());
        assertEquals(createTransferResult2, batch.getStatus());
        assertEquals(0L, batch.getReserved());

        assertFalse(batch.next());
    }

    @Test
    public void testWriteCreateTransferStatuss() {
        var batch = new CreateTransferResultBatch(1);
        batch.add();

        batch.setTimestamp(createTransferTimestamp1);
        assertEquals(createTransferTimestamp1, batch.getTimestamp());

        batch.setStatus(createTransferResult1);
        assertEquals(createTransferResult1, batch.getStatus());

        batch.setReserved(100);
        assertEquals(100, batch.getReserved());
    }

    @Test(expected = AssertionError.class)
    public void testInvalidTransferAccountResultsBuffer() {

        // Invalid size
        var invalidBuffer = ByteBuffer.allocate((CreateTransferResultBatch.Struct.SIZE * 2) - 1)
                .order(ByteOrder.LITTLE_ENDIAN);

        @SuppressWarnings("unused")
        var batch = new CreateTransferResultBatch(invalidBuffer);
        assert false;
    }

    @Test
    public void testReadIds() {

        var batch = new IdBatch(dummyIdsStream.position(0));
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());

        assertTrue(batch.next());
        assertEquals(id1LeastSignificant, batch.getId(UInt128.LeastSignificant));
        assertEquals(id1MostSignificant, batch.getId(UInt128.MostSignificant));
        assertArrayEquals(id1, batch.getId());

        assertTrue(batch.next());
        assertEquals(id2LeastSignificant, batch.getId(UInt128.LeastSignificant));
        assertEquals(id2MostSignificant, batch.getId(UInt128.MostSignificant));
        assertArrayEquals(id2, batch.getId());

        assertFalse(batch.next());
    }

    @Test
    public void testWriteIds() {

        var batch = new IdBatch(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        batch.add(id1LeastSignificant, id1MostSignificant);
        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());

        batch.add(id2LeastSignificant, id2MostSignificant);
        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());

        batch.beforeFirst();

        assertTrue(batch.next());
        assertEquals(id1LeastSignificant, batch.getId(UInt128.LeastSignificant));
        assertEquals(id1MostSignificant, batch.getId(UInt128.MostSignificant));
        assertArrayEquals(id1, batch.getId());

        assertTrue(batch.next());
        assertEquals(id2LeastSignificant, batch.getId(UInt128.LeastSignificant));
        assertEquals(id2MostSignificant, batch.getId(UInt128.MostSignificant));
        assertArrayEquals(id2, batch.getId());

        assertFalse(batch.next());

        assertBuffer(dummyIdsStream, batch.getBuffer());
    }

    @Test
    public void testMoveAndSetIds() {

        var batch = new IdBatch(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        // Set index 0
        batch.add(id1);
        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(id1LeastSignificant, batch.getId(UInt128.LeastSignificant));
        assertEquals(id1MostSignificant, batch.getId(UInt128.MostSignificant));
        assertArrayEquals(id1, batch.getId());

        // Set index 1 with id1 again
        batch.add(id1);
        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(id1LeastSignificant, batch.getId(UInt128.LeastSignificant));
        assertEquals(id1MostSignificant, batch.getId(UInt128.MostSignificant));
        assertArrayEquals(id1, batch.getId());

        // Replace index 0 with id2
        batch.setPosition(0);
        assertEquals(0, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        batch.setId(id2);

        batch.beforeFirst();

        assertTrue(batch.next());
        assertArrayEquals(id2, batch.getId());

        assertTrue(batch.next());
        assertArrayEquals(id1, batch.getId());

        assertFalse(batch.next());
    }

    @Test(expected = AssertionError.class)
    public void testInvalidIdsBuffer() {

        // Invalid size
        var invalidBuffer =
                ByteBuffer.allocate((UInt128.SIZE * 2) - 1).order(ByteOrder.LITTLE_ENDIAN);

        @SuppressWarnings("unused")
        var batch = new IdBatch(invalidBuffer);
        assert false;
    }

    @Test(expected = NullPointerException.class)
    public void testNullIds() {

        var batch = new IdBatch(1);
        batch.add();
        batch.setId(null);
        assert false;
    }

    @Test
    public void testLongIds() {
        var batch = new IdBatch(1);
        batch.add(100L);
        assertEquals(100L, batch.getId(UInt128.LeastSignificant));
        assertEquals(0L, batch.getId(UInt128.MostSignificant));
    }

    private static void setAccount(AccountBatch batch, DummyAccountDto account) {
        batch.setId(account.idLeastSignificant, account.idMostSignificant);
        batch.setDebitsPending(account.debitsPending);
        batch.setDebitsPosted(account.debitsPosted);
        batch.setCreditsPending(account.creditsPending);
        batch.setCreditsPosted(account.creditsPosted);
        batch.setUserData128(account.userData128LeastSignificant,
                account.userData128MostSignificant);
        batch.setUserData64(account.userData64);
        batch.setUserData32(account.userData32);
        batch.setLedger(account.ledger);
        batch.setCode(account.code);
        batch.setFlags(account.flags);
        batch.setTimestamp(account.timestamp);
    }

    private static void assertAccounts(DummyAccountDto account, AccountBatch batch) {
        assertEquals(account.idLeastSignificant, batch.getId(UInt128.LeastSignificant));
        assertEquals(account.idMostSignificant, batch.getId(UInt128.MostSignificant));
        assertEquals(account.debitsPending, batch.getDebitsPending());
        assertEquals(account.debitsPosted, batch.getDebitsPosted());
        assertEquals(account.creditsPending, batch.getCreditsPending());
        assertEquals(account.creditsPosted, batch.getCreditsPosted());
        assertEquals(account.userData128LeastSignificant,
                batch.getUserData128(UInt128.LeastSignificant));
        assertEquals(account.userData128MostSignificant,
                batch.getUserData128(UInt128.MostSignificant));
        assertEquals(account.userData64, batch.getUserData64());
        assertEquals(account.userData32, batch.getUserData32());
        assertEquals(account.ledger, batch.getLedger());
        assertEquals(account.code, (short) batch.getCode());
        assertEquals(account.flags, (short) batch.getFlags());
        assertEquals(account.timestamp, batch.getTimestamp());
    }

    private static void assertTransfers(DummyTransferDto transfer, TransferBatch batch) {
        assertEquals(transfer.idLeastSignificant, batch.getId(UInt128.LeastSignificant));
        assertEquals(transfer.idMostSignificant, batch.getId(UInt128.MostSignificant));
        assertEquals(transfer.creditAccountIdLeastSignificant,
                batch.getCreditAccountId(UInt128.LeastSignificant));
        assertEquals(transfer.creditAccountIdMostSignificant,
                batch.getCreditAccountId(UInt128.MostSignificant));
        assertEquals(transfer.debitAccountIdLeastSignificant,
                batch.getDebitAccountId(UInt128.LeastSignificant));
        assertEquals(transfer.debitAccountIdMostSignificant,
                batch.getDebitAccountId(UInt128.MostSignificant));
        assertEquals(transfer.amount, batch.getAmount());
        assertEquals(transfer.pendingIdLeastSignificant,
                batch.getPendingId(UInt128.LeastSignificant));
        assertEquals(transfer.pendingIdMostSignificant,
                batch.getPendingId(UInt128.MostSignificant));
        assertEquals(transfer.userData128LeastSignificant,
                batch.getUserData128(UInt128.LeastSignificant));
        assertEquals(transfer.userData128MostSignificant,
                batch.getUserData128(UInt128.MostSignificant));
        assertEquals(transfer.userData64, batch.getUserData64());
        assertEquals(transfer.userData32, batch.getUserData32());
        assertEquals(transfer.ledger, batch.getLedger());
        assertEquals(transfer.code, (short) batch.getCode());
        assertEquals(transfer.flags, (short) batch.getFlags());
        assertEquals(transfer.timeout, batch.getTimeout());
        assertEquals(transfer.timestamp, batch.getTimestamp());
    }

    private static void setTransfer(TransferBatch batch, DummyTransferDto transfer) {
        batch.setId(transfer.idLeastSignificant, transfer.idMostSignificant);
        batch.setDebitAccountId(transfer.debitAccountIdLeastSignificant,
                transfer.debitAccountIdMostSignificant);
        batch.setCreditAccountId(transfer.creditAccountIdLeastSignificant,
                transfer.creditAccountIdMostSignificant);
        batch.setAmount(transfer.amount);
        batch.setPendingId(transfer.pendingIdLeastSignificant, transfer.pendingIdMostSignificant);
        batch.setUserData128(transfer.userData128LeastSignificant,
                transfer.userData128MostSignificant);
        batch.setUserData64(transfer.userData64);
        batch.setUserData32(transfer.userData32);
        batch.setLedger(transfer.ledger);
        batch.setCode(transfer.code);
        batch.setFlags(transfer.flags);
        batch.setTimeout(transfer.timeout);
        batch.setTimestamp(transfer.timestamp);
    }

    private void assertBuffer(ByteBuffer expected, ByteBuffer actual) {
        assertEquals(expected.capacity(), actual.capacity());
        for (int i = 0; i < expected.capacity(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }

    private static final class DummyAccountDto {
        public long idLeastSignificant;
        public long idMostSignificant;
        public BigInteger creditsPosted;
        public BigInteger creditsPending;
        public BigInteger debitsPosted;
        public BigInteger debitsPending;
        public long userData128LeastSignificant;
        public long userData128MostSignificant;
        public long userData64;
        public int userData32;
        public int ledger;
        public short code;
        public short flags;
        public long timestamp;
    }

    private static final class DummyTransferDto {
        private long idLeastSignificant;
        private long idMostSignificant;
        private long debitAccountIdLeastSignificant;
        private long debitAccountIdMostSignificant;
        private long creditAccountIdLeastSignificant;
        private long creditAccountIdMostSignificant;
        private BigInteger amount;
        private long pendingIdLeastSignificant;
        private long pendingIdMostSignificant;
        private long userData128LeastSignificant;
        private long userData128MostSignificant;
        private long userData64;
        private int userData32;
        private int timeout;
        private int ledger;
        private short code;
        private short flags;
        private long timestamp;
    }

    static {

        account1 = new DummyAccountDto();
        account1.idLeastSignificant = 10;
        account1.idMostSignificant = 100;
        account1.debitsPending = BigInteger.valueOf(100);
        account1.debitsPosted = BigInteger.valueOf(200);
        account1.creditsPending = BigInteger.valueOf(300);
        account1.creditsPosted = BigInteger.valueOf(400);
        account1.userData128LeastSignificant = 1000;
        account1.userData128MostSignificant = 1100;
        account1.userData64 = 2000;
        account1.userData32 = 3000;
        account1.ledger = 720;
        account1.code = 1;
        account1.flags = AccountFlags.LINKED;
        account1.timestamp = 999;

        account2 = new DummyAccountDto();
        account2.idLeastSignificant = 20;
        account2.idMostSignificant = 200;
        account2.debitsPending = BigInteger.valueOf(10);
        account2.debitsPosted = BigInteger.valueOf(20);
        account2.creditsPending = BigInteger.valueOf(30);
        account2.creditsPosted = BigInteger.valueOf(40);
        account2.userData128LeastSignificant = 2000;
        account2.userData128MostSignificant = 2200;
        account2.userData64 = 4000;
        account2.userData32 = 5000;
        account2.ledger = 730;
        account2.code = 2;
        account2.flags = AccountFlags.LINKED | AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS;
        account2.timestamp = 99;

        // Mimic the binary response
        dummyAccountsStream = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);

        // Item 1
        dummyAccountsStream.putLong(10).putLong(100); // Id
        dummyAccountsStream.putLong(100).putLong(0); // DebitsPending
        dummyAccountsStream.putLong(200).putLong(0); // DebitsPosted
        dummyAccountsStream.putLong(300).putLong(0); // CreditPending
        dummyAccountsStream.putLong(400).putLong(0); // CreditsPosted
        dummyAccountsStream.putLong(1000).putLong(1100); // UserData128
        dummyAccountsStream.putLong(2000); // UserData64
        dummyAccountsStream.putInt(3000); // UserData32
        dummyAccountsStream.putInt(0); // Reserved
        dummyAccountsStream.putInt(720); // Ledger
        dummyAccountsStream.putShort((short) 1); // Code
        dummyAccountsStream.putShort((short) 1); // Flags
        dummyAccountsStream.putLong(999); // Timestamp

        // Item 2
        dummyAccountsStream.putLong(20).putLong(200); // Id
        dummyAccountsStream.putLong(10).putLong(0); // DebitsPending
        dummyAccountsStream.putLong(20).putLong(0);; // DebitsPosted
        dummyAccountsStream.putLong(30).putLong(0);; // CreditPending
        dummyAccountsStream.putLong(40).putLong(0);; // CreditsPosted
        dummyAccountsStream.putLong(2000).putLong(2200); // UserData128
        dummyAccountsStream.putLong(4000); // UserData64
        dummyAccountsStream.putInt(5000); // UserData32
        dummyAccountsStream.putInt(0); // Reserved
        dummyAccountsStream.putInt(730); // Ledger
        dummyAccountsStream.putShort((short) 2); // Code
        dummyAccountsStream.putShort((short) 5); // Flags
        dummyAccountsStream.putLong(99); // Timestamp

        transfer1 = new DummyTransferDto();
        transfer1.idLeastSignificant = 5000;
        transfer1.idMostSignificant = 500;
        transfer1.debitAccountIdLeastSignificant = 1000;
        transfer1.debitAccountIdMostSignificant = 100;
        transfer1.creditAccountIdLeastSignificant = 2000;
        transfer1.creditAccountIdMostSignificant = 200;
        transfer1.amount = BigInteger.valueOf(1000);
        transfer1.userData128LeastSignificant = 3000;
        transfer1.userData128MostSignificant = 300;
        transfer1.userData64 = 6000;
        transfer1.userData32 = 7000;
        transfer1.code = 10;
        transfer1.ledger = 720;

        transfer2 = new DummyTransferDto();
        transfer2.idLeastSignificant = 5001;
        transfer2.idMostSignificant = 501;
        transfer2.debitAccountIdLeastSignificant = 1001;
        transfer2.debitAccountIdMostSignificant = 101;
        transfer2.creditAccountIdLeastSignificant = 2001;
        transfer2.creditAccountIdMostSignificant = 201;
        transfer2.amount = BigInteger.valueOf(200);
        transfer2.pendingIdLeastSignificant = transfer1.idLeastSignificant;
        transfer2.pendingIdMostSignificant = transfer1.idMostSignificant;
        transfer2.userData128LeastSignificant = 3001;
        transfer2.userData128MostSignificant = 301;
        transfer2.userData64 = 8000;
        transfer2.userData32 = 9000;
        transfer2.timeout = 2500;
        transfer2.code = 20;
        transfer2.ledger = 100;
        transfer2.flags = TransferFlags.PENDING | TransferFlags.LINKED;
        transfer2.timestamp = 900;

        // Mimic the binary response
        dummyTransfersStream = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);

        // Item 1
        dummyTransfersStream.putLong(5000).putLong(500); // Id
        dummyTransfersStream.putLong(1000).putLong(100); // CreditAccountId
        dummyTransfersStream.putLong(2000).putLong(200); // DebitAccountId
        dummyTransfersStream.putLong(1000).putLong(0); // Amount
        dummyTransfersStream.putLong(0).putLong(0); // PendingId
        dummyTransfersStream.putLong(3000).putLong(300); // UserData128
        dummyTransfersStream.putLong(6000); // UserData64
        dummyTransfersStream.putInt(7000); // UserData32
        dummyTransfersStream.putInt(0); // Timeout
        dummyTransfersStream.putInt(720); // Ledger
        dummyTransfersStream.putShort((short) 10); // Code
        dummyTransfersStream.putShort((short) 0); // Flags
        dummyTransfersStream.putLong(0); // Timestamp

        // Item 2
        dummyTransfersStream.putLong(5001).putLong(501); // Id
        dummyTransfersStream.putLong(1001).putLong(101); // CreditAccountId
        dummyTransfersStream.putLong(2001).putLong(201); // DebitAccountId
        dummyTransfersStream.putLong(200).putLong(0); // Amount
        dummyTransfersStream.putLong(5000).putLong(500); // PendingId
        dummyTransfersStream.putLong(3001).putLong(301); // UserData128
        dummyTransfersStream.putLong(8000); // UserData64
        dummyTransfersStream.putInt(9000); // UserData32
        dummyTransfersStream.putInt(2500); // Timeout
        dummyTransfersStream.putInt(100); // Ledger
        dummyTransfersStream.putShort((short) 20); // Code
        dummyTransfersStream.putShort((short) 3); // Flags
        dummyTransfersStream.putLong(900); // Timestamp

        createAccountTimestamp1 = 999_998;
        createAccountTimestamp2 = 999_999;
        createAccountResult1 = CreateAccountStatus.Created;
        createAccountResult2 = CreateAccountStatus.Exists;

        // Mimic the binary response
        dummyCreateAccountStatussStream = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        dummyCreateAccountStatussStream.putLong(createAccountTimestamp1)
                .putInt(createAccountResult1.value).putInt(0);
        dummyCreateAccountStatussStream.putLong(createAccountTimestamp2)
                .putInt(createAccountResult2.value).putInt(0);

        createTransferTimestamp1 = 999_998;
        createTransferTimestamp2 = 999_999;
        createTransferResult1 = CreateTransferStatus.Created;
        createTransferResult2 = CreateTransferStatus.ExceedsDebits;

        // Mimic the binary response
        dummyCreateTransferResultsStream = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        dummyCreateTransferResultsStream.putLong(createTransferTimestamp1)
                .putInt(createTransferResult1.value).putInt(0);
        dummyCreateTransferResultsStream.putLong(createTransferTimestamp2)
                .putInt(createTransferResult2.value).putInt(0);

        id1 = new byte[] {10, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0};
        id1LeastSignificant = 10;
        id1MostSignificant = 100;
        id2 = new byte[] {2, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0};
        id2LeastSignificant = 2;
        id2MostSignificant = 20;

        // Mimic the binary response
        dummyIdsStream = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        dummyIdsStream.putLong(10).putLong(100); // Item (10,100)
        dummyIdsStream.putLong(2).putLong(20); // Item (2,20)
    }
}
