package com.tigerbeetle;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

import org.junit.Test;

/**
 * Asserts the memory interpretation from/to a binary stream.
 */
public class UUIDBatchTest {

    private static final UUID uuid1;
    private static final UUID uuid2;
    private static final ByteBuffer dummyStream;

    static {

        uuid1 = new UUID(1000, 100);
        uuid2 = new UUID(2000, 200);

        // Mimic the the binnary response
        dummyStream = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        dummyStream.putLong(100).putLong(1000); // Item 1
        dummyStream.putLong(200).putLong(2000); // Item 2
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeCapacity() {
        new UUIDsBatch(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullBuffer() {
        ByteBuffer buffer = null;
        new UUIDsBatch(buffer);
    }

    @Test
    public void testAdd() {

        UUIDsBatch batch = new UUIDsBatch(2);
        assertEquals(0, batch.getLenght());
        assertEquals(2, batch.getCapacity());

        batch.add(uuid1);
        assertEquals(1, batch.getLenght());

        batch.add(uuid2);
        assertEquals(2, batch.getLenght());

        UUID[] array = batch.toArray();
        assertEquals(array.length, 2);
        assertEquals(uuid1, array[0]);
        assertEquals(uuid2, array[1]);

        assertBuffer(dummyStream, batch.getBuffer());
    }

    @Test
    public void testGetAndSet() {

        UUIDsBatch batch = new UUIDsBatch(1);
        assertEquals(0, batch.getLenght());
        assertEquals(1, batch.getCapacity());

        batch.set(0, uuid1);
        assertEquals(1, batch.getLenght());

        assertEquals(uuid1, batch.get(0));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexOutOfBounds() {

        UUIDsBatch batch = new UUIDsBatch(1);
        batch.set(1, uuid1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexNegative() {

        UUIDsBatch batch = new UUIDsBatch(1);
        batch.set(-1, uuid1);
        assert false; // Should be unreachable
    }

    @Test(expected = NullPointerException.class)
    public void testSetNull() {

        UUIDsBatch batch = new UUIDsBatch(1);
        batch.set(0, null);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexOutOfBounds() {

        UUIDsBatch batch = new UUIDsBatch(1);
        batch.get(1);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexNegative() {

        UUIDsBatch batch = new UUIDsBatch(1);
        batch.get(-1);
        assert false; // Should be unreachable
    }

    @Test
    public void testFromArray() {

        UUIDsBatch batch = new UUIDsBatch(new UUID[] {uuid1, uuid2});
        assertEquals(batch.getLenght(), 2);

        assertBuffer(dummyStream, batch.getBuffer());
    }

    @Test
    public void testBufferLen() {
        var batch = new UUIDsBatch(dummyStream.position(0));
        assertEquals(dummyStream.capacity(), batch.getBufferLen());
    }

    @Test
    public void testToArray() {

        UUIDsBatch batch = new UUIDsBatch(dummyStream.position(0));
        assertEquals(batch.getLenght(), 2);

        UUID[] array = batch.toArray();
        assertEquals(array.length, 2);
        assertEquals(uuid1, array[0]);
        assertEquals(uuid2, array[1]);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidBuffer() {

        // Invalid size
        var invalidBuffer = ByteBuffer.allocate((UUIDsBatch.Struct.SIZE * 2) - 1)
                .order(ByteOrder.LITTLE_ENDIAN);

        var batch = new UUIDsBatch(invalidBuffer);
        assert batch == null; // Should be unreachable
    }

    private void assertBuffer(ByteBuffer expected, ByteBuffer actual) {
        assertEquals(expected.capacity(), actual.capacity());
        for (int i = 0; i < expected.capacity(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }
}
