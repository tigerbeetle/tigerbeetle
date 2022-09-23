package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

import org.junit.Test;

/**
 * Asserts the memory interpretation from/to a binary stream.
 */
public class UUIDBatchTest {

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
        new UInt128Batch(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullBuffer() {
        ByteBuffer buffer = null;
        new UInt128Batch(buffer);
    }

    @Test
    public void testAdd() {

        UInt128Batch batch = new UInt128Batch(2);
        assertEquals(0, batch.getLenght());
        assertEquals(2, batch.getCapacity());

        batch.add(id1);
        assertEquals(1, batch.getLenght());

        batch.add(id2);
        assertEquals(2, batch.getLenght());

        byte[][] array = batch.toArray();
        assertEquals(array.length, 2);
        assertArrayEquals(id1, array[0]);
        assertArrayEquals(id2, array[1]);

        assertBuffer(dummyStream, batch.getBuffer());
    }

    @Test
    public void testGetAndSet() {

        UInt128Batch batch = new UInt128Batch(1);
        assertEquals(0, batch.getLenght());
        assertEquals(1, batch.getCapacity());

        batch.set(0, id1);
        assertEquals(1, batch.getLenght());

        assertArrayEquals(id1, batch.get(0));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexOutOfBounds() {

        UInt128Batch batch = new UInt128Batch(1);
        batch.set(1, id1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexNegative() {

        UInt128Batch batch = new UInt128Batch(1);
        batch.set(-1, id1);
        assert false; // Should be unreachable
    }

    @Test(expected = NullPointerException.class)
    public void testSetNull() {
        UUID uuid = null;
        UInt128Batch batch = new UInt128Batch(1);
        batch.set(0, uuid);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexOutOfBounds() {

        UInt128Batch batch = new UInt128Batch(1);
        batch.get(1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexNegative() {

        UInt128Batch batch = new UInt128Batch(1);
        batch.get(-1);
        assert false; // Should be unreachable
    }

    @Test
    public void testFromArray() {

        UInt128Batch batch = new UInt128Batch(new byte[][] {id1, id2});
        assertEquals(batch.getLenght(), 2);

        assertBuffer(dummyStream, batch.getBuffer());
    }

    @Test
    public void testBufferLen() {
        var batch = new UInt128Batch(dummyStream.position(0));
        assertEquals(dummyStream.capacity(), batch.getBufferLen());
    }

    @Test
    public void testToArray() {

        UInt128Batch batch = new UInt128Batch(dummyStream.position(0));
        assertEquals(batch.getLenght(), 2);

        byte[][] array = batch.toArray();
        assertEquals(array.length, 2);
        assertArrayEquals(id1, array[0]);
        assertArrayEquals(id2, array[1]);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidBuffer() {

        // Invalid size
        var invalidBuffer = ByteBuffer.allocate((UInt128Batch.Struct.SIZE * 2) - 1)
                .order(ByteOrder.LITTLE_ENDIAN);

        var batch = new UInt128Batch(invalidBuffer);
        assert batch == null; // Should be unreachable
    }

    private void assertBuffer(ByteBuffer expected, ByteBuffer actual) {
        assertEquals(expected.capacity(), actual.capacity());
        for (int i = 0; i < expected.capacity(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }
}
