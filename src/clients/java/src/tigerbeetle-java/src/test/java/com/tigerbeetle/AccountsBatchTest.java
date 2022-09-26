package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;
import com.tigerbeetle.UInt128.Bytes;

/**
 * Asserts the memory interpretation from/to a binary stream.
 */
public class AccountsBatchTest {

    private static final class AccountData {
        public long idLeastSignificant;
        public long idMostSignificant;
        public long userDataLeastSignificant;
        public long userDataMostSignificant;
        public int ledger;
        public short code;
        public short flags;
        public long creditsPosted;
        public long creditsPending;
        public long debitsPosted;
        public long debitsPending;
        public long timestamp;
    }

    private static final AccountData account1;
    private static final AccountData account2;
    private static final ByteBuffer dummyStream;

    static {

        account1 = new AccountData();
        account1.idLeastSignificant = 10;
        account1.idMostSignificant = 100;
        account1.userDataLeastSignificant = 1000;
        account1.userDataMostSignificant = 1100;
        account1.ledger = 720;
        account1.code = 1;
        account1.flags = AccountFlags.LINKED;
        account1.debitsPending = 100;
        account1.debitsPosted = 200;
        account1.creditsPending = 300;
        account1.creditsPosted = 400;
        account1.timestamp = 999;

        account2 = new AccountData();
        account2.idLeastSignificant = 20;
        account2.idMostSignificant = 200;
        account2.userDataLeastSignificant = 2000;
        account2.userDataMostSignificant = 2200;
        account2.ledger = 730;
        account2.code = 2;
        account2.flags = AccountFlags.LINKED | AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS;
        account2.debitsPending = 10;
        account2.debitsPosted = 20;
        account2.creditsPending = 30;
        account2.creditsPosted = 40;
        account2.timestamp = 99;

        // Mimic the the binnary response
        dummyStream = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);

        // Item 1
        dummyStream.putLong(10).putLong(100); // Id
        dummyStream.putLong(1000).putLong(1100); // UserData
        dummyStream.put(new byte[48]); // Reserved
        dummyStream.putInt(720); // Ledger
        dummyStream.putShort((short) 1); // Code
        dummyStream.putShort((short) 1); // Flags
        dummyStream.putLong(100); // DebitsPending
        dummyStream.putLong(200); // DebitsPosted
        dummyStream.putLong(300); // CreditPending
        dummyStream.putLong(400); // CreditsPosted
        dummyStream.putLong(999); // Timestamp

        // Item 2
        dummyStream.putLong(20).putLong(200); // Id
        dummyStream.putLong(2000).putLong(2200); // UserData
        dummyStream.put(new byte[48]); // Reserved
        dummyStream.putInt(730); // Ledger
        dummyStream.putShort((short) 2); // Code
        dummyStream.putShort((short) 5); // Flags
        dummyStream.putLong(10); // DebitsPending
        dummyStream.putLong(20); // DebitsPosted
        dummyStream.putLong(30); // CreditPending
        dummyStream.putLong(40); // CreditsPosted
        dummyStream.putLong(99); // Timestamp
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeCapacity() {
        new Accounts(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullBuffer() {
        ByteBuffer buffer = null;
        new Accounts(buffer);
    }


    @Test(expected = IndexOutOfBoundsException.class)
    public void testPositionIndexOutOfBounds() {

        Accounts batch = new Accounts(1);
        batch.setPosition(1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testPositionIndexNegative() {

        Accounts batch = new Accounts(1);
        batch.setPosition(-1);
        assert false; // Should be unreachable
    }

    @Test
    public void testNextFromCapacity() {
        var batch = new Accounts(2);

        // Creating from capacity
        // Expected position = -1 and length = 0
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(0 * Accounts.Struct.SIZE, batch.getBufferLen());

        assertFalse(batch.isValidPosition());

        // Zero elements, next must return false
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());

        // Calling multiple times must return false
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());

        // Adding 2 elements

        batch.add();
        assertTrue(batch.isValidPosition());
        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        assertEquals(1 * Accounts.Struct.SIZE, batch.getBufferLen());

        batch.add();
        assertTrue(batch.isValidPosition());
        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2 * Accounts.Struct.SIZE, batch.getBufferLen());

        // reset to the beginning,
        // Expected position -1
        batch.reset();
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

        // Calling multiple times must return false
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());
    }

    @Test
    public void testNextFromBuffer() {
        var batch = new Accounts(dummyStream.position(0));

        // Creating from a existing buffer
        // Expected position = -1 and length = 2
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(dummyStream.capacity(), batch.getBufferLen());
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

        // Calling multiple times must return false
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());
    }

    @Test
    public void testNextEmptyBatch() {
        var batch = Accounts.EMPTY;

        // Empty batch
        // Expected position = -1 and length = 0
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(0, batch.getCapacity());
        assertEquals(0, batch.getBufferLen());
        assertFalse(batch.isValidPosition());

        // End of the batch
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());

        // Reseting an empty batch
        batch.reset();

        // Still at the end of the batch
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());

        // Calling multiple times must return false
        assertFalse(batch.next());
        assertFalse(batch.isValidPosition());

    }

    @Test
    public void testAdd() {
        var batch = new Accounts(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(0 * Accounts.Struct.SIZE, batch.getBufferLen());

        batch.add();

        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(1 * Accounts.Struct.SIZE, batch.getBufferLen());

        batch.add();

        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(2 * Accounts.Struct.SIZE, batch.getBufferLen());
    }

    @Test(expected = IllegalStateException.class)
    public void testAddReadOnly() {
        var batch = new Accounts(dummyStream.asReadOnlyBuffer().position(0));
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(2 * Accounts.Struct.SIZE, batch.getBufferLen());

        batch.add();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testAddExceedCapacity() {
        var batch = new Accounts(1);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(1, batch.getCapacity());
        assertEquals(0 * Accounts.Struct.SIZE, batch.getBufferLen());

        batch.add();

        assertEquals(0, batch.getPosition());
        assertEquals(1, batch.getLength());
        assertEquals(1 * Accounts.Struct.SIZE, batch.getBufferLen());

        batch.add();
    }

    @Test
    public void testRead() {

        Accounts batch = new Accounts(dummyStream.position(0));
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());

        assertTrue(batch.next());
        assertAccounts(account1, batch);

        assertTrue(batch.next());
        assertAccounts(account2, batch);

        assertFalse(batch.next());
    }

    @Test(expected = IllegalStateException.class)
    public void testReadInvalidPosition() {

        Accounts batch = new Accounts(dummyStream.position(0));
        assertEquals(-1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2, batch.getCapacity());

        assertFalse(batch.isValidPosition());
        batch.getLedger();
    }

    @Test
    public void testWrite() {

        Accounts batch = new Accounts(2);
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

        batch.reset();

        assertTrue(batch.next());
        assertAccounts(account1, batch);

        assertTrue(batch.next());
        assertAccounts(account2, batch);

        assertFalse(batch.next());

        assertBuffer(dummyStream, batch.getBuffer());
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteInvalidPosition() {

        Accounts batch = new Accounts(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        assertFalse(batch.isValidPosition());
        batch.setCode(100);
    }

    @Test
    public void testGetAndSet() {

        Accounts batch = new Accounts(2);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        // Set inndex 0
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

        batch.reset();

        assertTrue(batch.next());
        assertAccounts(account2, batch);

        assertTrue(batch.next());
        assertAccounts(account1, batch);

        assertFalse(batch.next());
    }

    @Test(expected = AssertionError.class)
    public void testInvalidBuffer() {

        // Invalid size
        var invalidBuffer =
                ByteBuffer.allocate((Accounts.Struct.SIZE * 2) - 1).order(ByteOrder.LITTLE_ENDIAN);

        @SuppressWarnings("unused")
        var batch = new Accounts(invalidBuffer);
        assert false;
    }

    private static void setAccount(Accounts batch, AccountData account) {
        batch.setId(account.idLeastSignificant, account.idMostSignificant);
        batch.setUserData(account.userDataLeastSignificant, account.userDataMostSignificant);
        batch.setLedger(account.ledger);
        batch.setCode(account.code);
        batch.setFlags(account.flags);
        batch.setDebitsPending(account.debitsPending);
        batch.setDebitsPosted(account.debitsPosted);
        batch.setCreditsPending(account.creditsPending);
        batch.setCreditsPosted(account.creditsPosted);
        batch.setTimestamp(account.timestamp);
    }

    private static void assertAccounts(AccountData account, Accounts batch) {
        assertEquals(account.idLeastSignificant, batch.getId(Bytes.LeastSignificant));
        assertEquals(account.idMostSignificant, batch.getId(Bytes.MostSignificant));
        assertEquals(account.userDataLeastSignificant, batch.getUserData(Bytes.LeastSignificant));
        assertEquals(account.userDataMostSignificant, batch.getUserData(Bytes.MostSignificant));
        assertEquals(account.ledger, batch.getLedger());
        assertEquals(account.code, (short) batch.getCode());
        assertEquals(account.flags, (short) batch.getFlags());
        assertEquals(account.debitsPending, batch.getDebitsPending());
        assertEquals(account.debitsPosted, batch.getDebitsPosted());
        assertEquals(account.creditsPending, batch.getCreditsPending());
        assertEquals(account.creditsPosted, batch.getCreditsPosted());
        assertEquals(account.timestamp, batch.getTimestamp());
    }

    private void assertBuffer(ByteBuffer expected, ByteBuffer actual) {
        assertEquals(expected.capacity(), actual.capacity());
        for (int i = 0; i < expected.capacity(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }
}
