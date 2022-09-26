package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

import org.junit.Test;

/**
 * Asserts the memory interpretation from/to a binary stream.
 */
public class IdsTest {

    private static final byte[] id1;
    private static final byte[] id2;
    private static final ByteBuffer dummyStream;

    static {

        id1 = UInt128.toBytes(100, 1000);
        id2 = UInt128.toBytes(200, 2000);

        // Mimic the the binnary response
        dummyStream = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        dummyStream.putLong(100).putLong(1000); // Item 1
        dummyStream.putLong(200).putLong(2000); // Item 2
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeCapacity() {
        new Ids(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullBuffer() {
        ByteBuffer buffer = null;
        new Ids(buffer);
    }

    @Test
    public void testAdd() {

        Ids batch = new Ids(2);
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());

        batch.add();
        batch.setId(id1);
        assertEquals(1, batch.getLength());

        batch.add();
        batch.setId(id2);
        assertEquals(2, batch.getLength());

        assertBuffer(dummyStream, batch.getBuffer());
    }

    @Test
    public void testGetAndSet() {

        Ids batch = new Ids(1);
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(1, batch.getCapacity());

        batch.add();
        assertTrue(batch.isValidPosition());
        batch.setId(id1);
        assertEquals(1, batch.getLength());

        assertArrayEquals(id1, batch.getId());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testPositionIndexOutOfBounds() {

        Ids batch = new Ids(1);
        batch.setPosition(1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testPositionIndexNegative() {

        Ids batch = new Ids(1);
        batch.setPosition(-1);
        assert false; // Should be unreachable
    }

    @Test(expected = NullPointerException.class)
    public void testSetNull() {
        var batch = new Ids(1);
        batch.add();
        batch.setId(null);
        assert false; // Should be unreachable
    }

    @Test
    public void testNextFromCapacity() {
        var batch = new Ids(2);

        // Creating from capacity
        // Expected position = -1 and length = 0
        assertEquals(-1, batch.getPosition());
        assertEquals(0, batch.getLength());
        assertEquals(2, batch.getCapacity());
        assertEquals(0 * UInt128.Bytes.SIZE, batch.getBufferLen());

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
        assertEquals(1 * UInt128.Bytes.SIZE, batch.getBufferLen());

        batch.add();
        assertTrue(batch.isValidPosition());
        assertEquals(1, batch.getPosition());
        assertEquals(2, batch.getLength());
        assertEquals(2 * UInt128.Bytes.SIZE, batch.getBufferLen());

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
        var batch = new Ids(dummyStream.position(0));

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
        var batch = Ids.EMPTY;

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

    @Test(expected = AssertionError.class)
    public void testInvalidBuffer() {

        // Invalid size
        var invalidBuffer =
                ByteBuffer.allocate((Ids.Struct.SIZE * 2) - 1).order(ByteOrder.LITTLE_ENDIAN);

        @SuppressWarnings("unused")
        var batch = new Ids(invalidBuffer);
        assert false; // Should be unreachable
    }

    private void assertBuffer(ByteBuffer expected, ByteBuffer actual) {
        assertEquals(expected.capacity(), actual.capacity());
        for (int i = 0; i < expected.capacity(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }
}
