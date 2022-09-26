package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;

/**
 * Asserts the memory interpretation from/to a binary stream.
 */
public class TransfersBatchTest {

    private static final Transfer transfer1;
    private static final Transfer transfer2;
    private static final ByteBuffer dummyStream;

    static {
        transfer1 = new Transfer();
        transfer1.setId(5000, 500);
        transfer1.setDebitAccountId(1000, 100);
        transfer1.setCreditAccountId(2000, 200);
        transfer1.setUserData(3000, 300);
        transfer1.setAmount(1000);
        transfer1.setCode(10);
        transfer1.setLedger(720);

        transfer2 = new Transfer();
        transfer2.setId(5001, 501);
        transfer2.setDebitAccountId(1001, 101);
        transfer2.setCreditAccountId(2001, 201);
        transfer2.setUserData(3001, 301);
        transfer2.setAmount(200);
        transfer2.setCode(20);
        transfer2.setLedger(100);
        transfer2.setFlags(TransferFlags.PENDING | TransferFlags.LINKED);
        transfer2.setPendingId(transfer1.getId());
        transfer2.setTimeout(2500);
        transfer2.setTimestamp(900);

        // Mimic the the binnary response
        dummyStream = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);

        // Item 1
        dummyStream.putLong(5000).putLong(500); // Id
        dummyStream.putLong(1000).putLong(100); // CreditAccountId
        dummyStream.putLong(2000).putLong(200); // DebitAccountId
        dummyStream.putLong(3000).putLong(300); // UserData
        dummyStream.put(new byte[16]); // Reserved
        dummyStream.putLong(0).putLong(0); // PendingId
        dummyStream.putLong(0); // Timeout
        dummyStream.putInt(720); // Ledger
        dummyStream.putShort((short) 10); // Code
        dummyStream.putShort((short) 0); // Flags
        dummyStream.putLong(1000); // Amount
        dummyStream.putLong(0); // Timestamp

        // Item 2
        dummyStream.putLong(5001).putLong(501); // Id
        dummyStream.putLong(1001).putLong(101); // CreditAccountId
        dummyStream.putLong(2001).putLong(201); // DebitAccountId
        dummyStream.putLong(3001).putLong(301); // UserData
        dummyStream.put(new byte[16]); // Reserved
        dummyStream.putLong(5000).putLong(500); // PendingId
        dummyStream.putLong(2500); // Timeout
        dummyStream.putInt(100); // Ledger
        dummyStream.putShort((short) 20); // Code
        dummyStream.putShort((short) 3); // Flags
        dummyStream.putLong(200); // Amount
        dummyStream.putLong(900); // Timestamp
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeCapacity() {
        new Transfers(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullBuffer() {
        ByteBuffer buffer = null;
        new Transfers(buffer);
    }

    @Test
    public void testGet() {

        Transfers batch = new Transfers(dummyStream.position(0));
        assertEquals(2, batch.getLength());

        Transfer getTransfer1 = batch.get(0);
        assertNotNull(getTransfer1);

        Transfer getTransfer2 = batch.get(1);
        assertNotNull(getTransfer2);

        assertTransfers(transfer1, getTransfer1);
        assertTransfers(transfer2, getTransfer2);
    }

    @Test
    public void testAdd() {

        Transfers batch = new Transfers(2);
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        batch.add(transfer1);
        assertEquals(1, batch.getLength());

        batch.add(transfer2);
        assertEquals(2, batch.getLength());

        Transfer getTransfer1 = batch.get(0);
        assertNotNull(getTransfer1);

        Transfer getTransfer2 = batch.get(1);
        assertNotNull(getTransfer2);

        assertTransfers(transfer1, getTransfer1);
        assertTransfers(transfer2, getTransfer2);

        assertBuffer(dummyStream, batch.getBuffer(0));
    }

    @Test
    public void testGetAndSet() {

        Transfers batch = new Transfers(2);
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        // Set inndex 0
        batch.set(0, transfer1);
        assertEquals(1, batch.getLength());

        Transfer getTransfer1 = batch.get(0);
        assertNotNull(getTransfer1);

        assertTransfers(transfer1, getTransfer1);

        // Set index 1
        batch.set(1, transfer1);
        assertEquals(2, batch.getLength());

        // Replace same index 0
        batch.set(0, transfer2);
        assertEquals(2, batch.getLength());

        Transfer getTransfer2 = batch.get(0);
        assertNotNull(getTransfer2);

        assertTransfers(transfer2, getTransfer2);

        // Assert if the index 1 remains unchanged
        Transfer getTransfer3 = batch.get(1);
        assertNotNull(getTransfer3);

        assertTransfers(transfer1, getTransfer3);
    }


    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexOutOfBounds() {

        Transfers batch = new Transfers(1);
        batch.set(1, transfer1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexNegative() {

        Transfers batch = new Transfers(1);
        batch.set(-1, transfer1);
        assert false; // Should be unreachable
    }

    @Test(expected = NullPointerException.class)
    public void testSetNull() {

        Transfers batch = new Transfers(1);
        batch.set(0, null);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexOutOfBounds() {

        Transfers batch = new Transfers(1);
        batch.get(1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexNegative() {

        Transfers batch = new Transfers(1);
        batch.get(-1);
        assert false; // Should be unreachable
    }

    @Test
    public void testFromArray() {

        Transfer[] array = new Transfer[] {transfer1, transfer2};

        Transfers batch = new Transfers(array);
        assertEquals(batch.getLength(), 2);
        assertEquals(batch.getCapacity(), 2);

        assertTransfers(transfer1, batch.get(0));
        assertTransfers(transfer2, batch.get(1));

        assertBuffer(dummyStream, batch.getBuffer(0));
    }

    @Test
    public void testToArray() {

        Transfers batch = new Transfers(dummyStream.position(0));
        assertEquals(2, batch.getLength());

        Transfer[] array = batch.toArray();
        assertEquals(2, array.length);
        assertTransfers(transfer1, array[0]);
        assertTransfers(transfer2, array[1]);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidBuffer() {

        // Invalid size
        var invalidBuffer =
                ByteBuffer.allocate((Transfer.Struct.SIZE * 2) - 1).order(ByteOrder.LITTLE_ENDIAN);

        var batch = new Transfers(invalidBuffer);
        assert batch == null; // Should be unreachable
    }

    @Test
    public void testBufferLen() {
        var batch = new Transfers(dummyStream.position(0));
        assertEquals(dummyStream.capacity(), batch.getBufferLen());
    }

    private static void assertTransfers(Transfer transfer1, Transfer transfer2) {
        assertArrayEquals(transfer1.getId(), transfer2.getId());
        assertArrayEquals(transfer1.getCreditAccountId(), transfer2.getCreditAccountId());
        assertArrayEquals(transfer1.getDebitAccountId(), transfer2.getDebitAccountId());
        assertArrayEquals(transfer1.getUserData(), transfer2.getUserData());
        assertEquals(transfer1.getLedger(), transfer2.getLedger());
        assertEquals(transfer1.getCode(), transfer2.getCode());
        assertEquals(transfer1.getFlags(), transfer2.getFlags());
        assertEquals(transfer1.getAmount(), transfer2.getAmount());
        assertEquals(transfer1.getTimeout(), transfer2.getTimeout());
        assertArrayEquals(transfer1.getPendingId(), transfer2.getPendingId());
        assertEquals(transfer1.getTimestamp(), transfer2.getTimestamp());
    }

    private void assertBuffer(ByteBuffer expected, ByteBuffer actual) {
        assertEquals(expected.capacity(), actual.capacity());
        for (int i = 0; i < expected.capacity(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }
}
