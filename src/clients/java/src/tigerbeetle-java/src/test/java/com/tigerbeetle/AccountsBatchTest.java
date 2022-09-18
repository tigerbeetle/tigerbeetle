package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

import org.junit.Test;

/**
 * Asserts the memory interpretation from/to a binary stream
 */
public class AccountsBatchTest {

    private static final Account account1;
    private static final Account account2;
    private static final ByteBuffer dummyStream;

    static {

        account1 = new Account();
        account1.setId(new UUID(10, 100));
        account1.setUserData(new UUID(1000, 1100));
        account1.setLedger(720);
        account1.setCode(1);
        account1.setFlags(AccountFlags.LINKED);
        account1.setDebitsPending(100);
        account1.setDebitsPosted(200);
        account1.setCreditsPending(300);
        account1.setCreditsPosted(400);
        account1.setTimestamp(999);

        account2 = new Account();
        account2.setId(new UUID(20, 200));
        account2.setUserData(new UUID(2000, 2200));
        account2.setLedger(730);
        account2.setCode(2);
        account2.setFlags(AccountFlags.LINKED | AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS);
        account2.setDebitsPending(10);
        account2.setDebitsPosted(20);
        account2.setCreditsPending(30);
        account2.setCreditsPosted(40);
        account2.setTimestamp(99);

        // Mimic the the binnary response
        dummyStream = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);

        // Item 1
        dummyStream.putLong(100).putLong(10); // Id
        dummyStream.putLong(1100).putLong(1000); // UserData
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
        dummyStream.putLong(200).putLong(20); // Id
        dummyStream.putLong(2200).putLong(2000); // UserData
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

    @Test
    public void testGet() {

        AccountsBatch batch = new AccountsBatch(dummyStream.position(0));
        assertEquals(2, batch.getLenght());

        Account getAccount1 = batch.get(0);
        assertNotNull(getAccount1);

        Account getAccount2 = batch.get(1);
        assertNotNull(getAccount2);

        assertAccounts(account1, getAccount1);
        assertAccounts(account2, getAccount2);
    }


    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexOutOfBounds() {

        AccountsBatch batch = new AccountsBatch(1);
        batch.set(1, account1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexNegative() {

        AccountsBatch batch = new AccountsBatch(1);
        batch.set(-1, account1);
        assert false; // Should be unreachable
    }

    @Test(expected = NullPointerException.class)
    public void testSetNull() {

        AccountsBatch batch = new AccountsBatch(1);
        batch.set(0, null);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexOutOfBounds() {

        AccountsBatch batch = new AccountsBatch(1);
        batch.get(1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexNegative() {

        AccountsBatch batch = new AccountsBatch(1);
        batch.get(-1);
        assert false; // Should be unreachable
    }

    @Test
    public void testAdd() {

        AccountsBatch batch = new AccountsBatch(2);
        assertEquals(0, batch.getLenght());
        assertEquals(2, batch.getCapacity());

        batch.add(account1);
        assertEquals(1, batch.getLenght());

        batch.add(account2);
        assertEquals(2, batch.getLenght());

        Account getAccount1 = batch.get(0);
        assertNotNull(getAccount1);

        Account getAccount2 = batch.get(1);
        assertNotNull(getAccount2);

        assertAccounts(account1, getAccount1);
        assertAccounts(account2, getAccount2);

        assertBuffer(dummyStream, batch.getBuffer());
    }

    @Test
    public void testGetAndSet() {

        AccountsBatch batch = new AccountsBatch(2);
        assertEquals(0, batch.getLenght());
        assertEquals(2, batch.getCapacity());

        // Set inndex 0
        batch.set(0, account1);
        assertEquals(1, batch.getLenght());

        Account getAccount1 = batch.get(0);
        assertNotNull(getAccount1);

        assertAccounts(account1, getAccount1);

        // Set index 1
        batch.set(1, account1);
        assertEquals(2, batch.getLenght());

        // Replace same index 0
        batch.set(0, account2);
        assertEquals(2, batch.getLenght());

        Account getAccount2 = batch.get(0);
        assertNotNull(getAccount2);

        assertAccounts(account2, getAccount2);

        // Assert if the index 1 remains unchanged
        Account getAccount1Again = batch.get(1);
        assertNotNull(getAccount1Again);

        assertAccounts(account1, getAccount1Again);
    }

    @Test
    public void testFromArray() {

        Account[] array = new Account[] {account1, account2};

        AccountsBatch batch = new AccountsBatch(array);
        assertEquals(2, batch.getLenght());
        assertEquals(2, batch.getCapacity());

        assertAccounts(account1, batch.get(0));
        assertAccounts(account2, batch.get(1));

        assertBuffer(dummyStream, batch.getBuffer());
    }

    @Test
    public void testToArray() {

        AccountsBatch batch = new AccountsBatch(dummyStream.position(0));
        assertEquals(2, batch.getLenght());

        Account[] array = batch.toArray();
        assertEquals(2, array.length);
        assertAccounts(account1, array[0]);
        assertAccounts(account2, array[1]);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidBuffer() {

        // Invalid size
        var invalidBuffer =
                ByteBuffer.allocate((Account.Struct.SIZE * 2) - 1).order(ByteOrder.LITTLE_ENDIAN);

        var batch = new AccountsBatch(invalidBuffer);
        assert batch == null; // Should be unreachable
    }

    @Test
    public void testBufferLen() {
        var batch = new AccountsBatch(dummyStream.position(0));
        assertEquals(dummyStream.capacity(), batch.getBufferLen());
    }

    private static void assertAccounts(Account account1, Account account2) {
        assertEquals(account1.getId(), account2.getId());
        assertEquals(account1.getUserData(), account2.getUserData());
        assertEquals(account1.getLedger(), account2.getLedger());
        assertEquals(account1.getCode(), account2.getCode());
        assertEquals(account1.getFlags(), account2.getFlags());
        assertEquals(account1.getDebitsPending(), account2.getDebitsPending());
        assertEquals(account1.getDebitsPosted(), account2.getDebitsPosted());
        assertEquals(account1.getCreditsPending(), account2.getCreditsPending());
        assertEquals(account1.getCreditsPosted(), account2.getCreditsPosted());
        assertEquals(account1.getTimestamp(), account2.getTimestamp());
    }

    private void assertBuffer(ByteBuffer expected, ByteBuffer actual) {
        assertEquals(expected.capacity(), actual.capacity());
        for (int i = 0; i < expected.capacity(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }
}
