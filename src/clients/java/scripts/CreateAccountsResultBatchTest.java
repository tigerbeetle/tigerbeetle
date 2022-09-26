package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;

/**
 * Asserts the memory interpretation from/to a binary stream.
 */
public class CreateAccountsResultBatchTest {

    private static final CreateAccountResult result1 = CreateAccountResult.Ok;
    private static final CreateAccountResult result2 = CreateAccountResult.Exists;
    private static final ByteBuffer dummyStream;

    static {
        // Mimic the the binnary response
        dummyStream = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        dummyStream.putInt(0).putInt(0); // Item 0 - OK
        dummyStream.putInt(1).putInt(21); // Item 1 - Exists
    }

    @Test
    public void testGet() {

        CreateAccountResults batch = new CreateAccountResults(dummyStream.position(0));
        assertEquals(2, batch.getCapacity());
        assertEquals(2, batch.getLength());
        assertEquals(0, batch.position());

        CreateAccountsResult getResult1 = batch.get(0);
        assertNotNull(getResult1);
        assertResults(result1, getResult1);

        CreateAccountsResult getResult2 = batch.get(1);
        assertNotNull(getResult2);
        assertResults(result2, getResult2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexOutOfBounds() {

        CreateAccountResults batch = new CreateAccountResults(dummyStream.position(0));
        batch.get(3);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexNegative() {

        CreateAccountResults batch = new CreateAccountResults(dummyStream.position(0));
        batch.get(-1);
        assert false; // Should be unreachable
    }

    @Test
    public void testToArray() {

        CreateAccountResults batch = new CreateAccountResults(dummyStream.position(0));
        assertEquals(batch.getCapacity(), 2);

        CreateAccountsResult[] array = batch.toArray();
        assertEquals(array.length, 2);
        assertResults(result1, array[0]);
        assertResults(result2, array[1]);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidBuffer() {

        // Invalid size
        var invalidBuffer = ByteBuffer.allocate((CreateAccountsResult.Struct.SIZE * 2) - 1)
                .order(ByteOrder.LITTLE_ENDIAN);

        var batch = new CreateAccountResults(invalidBuffer);
        assert batch == null; // Should be unreachable
    }

    @Test
    public void testBufferLen() {
        var batch = new CreateAccountResults(dummyStream.position(0));
        assertEquals(dummyStream.capacity(), batch.getBufferLen());
    }

    private static void assertResults(CreateAccountsResult result1, CreateAccountsResult result2) {
        assertEquals(result1.index, result2.index);
        assertEquals(result1.result, result2.result);
    }
}
