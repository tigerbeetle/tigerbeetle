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

    @Test
    public void testAdd() throws RequestException {

        UUIDsBatch batch = new UUIDsBatch(2);
        assertEquals(0, batch.getLenght());
        assertEquals(2, batch.getCapacity());

        batch.Add(uuid1);
        assertEquals(1, batch.getLenght());

        batch.Add(uuid2);
        assertEquals(2, batch.getLenght());

        UUID[] array = batch.toArray();
        assertEquals(array.length, 2);
        assertEquals(uuid1, array[0]);
        assertEquals(uuid2, array[1]);

        assertBuffer(dummyStream, batch.getBuffer());
    }

    @Test
    public void testFromArray() throws RequestException {

        UUIDsBatch batch = new UUIDsBatch(new UUID[] { uuid1, uuid2 });
        assertEquals(batch.getLenght(), 2);

        assertBuffer(dummyStream, batch.getBuffer());
    }

    @Test
    public void testToArray() throws RequestException {

        UUIDsBatch batch = new UUIDsBatch(dummyStream.position(0));
        assertEquals(batch.getLenght(), 2);

        UUID[] array = batch.toArray();
        assertEquals(array.length, 2);
        assertEquals(uuid1, array[0]);
        assertEquals(uuid2, array[1]);
    }

    private void assertBuffer(ByteBuffer expected, ByteBuffer actual) {
        assertEquals(expected.capacity(), actual.capacity());
        for (int i = 0; i < expected.capacity(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }
}
