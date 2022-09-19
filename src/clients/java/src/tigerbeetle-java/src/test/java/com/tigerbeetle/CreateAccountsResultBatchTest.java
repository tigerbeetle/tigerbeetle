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

    private static final CreateAccountsResult result1;
    private static final CreateAccountsResult result2;
    private static final ByteBuffer dummyStream;

    static {

        result1 = new CreateAccountsResult(0, CreateAccountResult.Ok);
        result2 = new CreateAccountsResult(1, CreateAccountResult.Exists);

        // Mimic the the binnary response
        dummyStream = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        dummyStream.putInt(0).putInt(0); // Item 0 - OK
        dummyStream.putInt(1).putInt(21); // Item 1 - Exists
    }

    @Test
    public void testGet() {

        CreateAccountsResultBatch batch = new CreateAccountsResultBatch(dummyStream.position(0));
        assertEquals(batch.getLenght(), 2);

        CreateAccountsResult getResult1 = batch.get(0);
        assertNotNull(getResult1);
        assertResults(result1, getResult1);

        CreateAccountsResult getResult2 = batch.get(1);
        assertNotNull(getResult2);
        assertResults(result2, getResult2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexOutOfBounds() {

        CreateAccountsResultBatch batch = new CreateAccountsResultBatch(dummyStream.position(0));
        batch.get(3);
        assert false; // Should be unreachable
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexNegative() {

        CreateAccountsResultBatch batch = new CreateAccountsResultBatch(dummyStream.position(0));
        batch.get(-1);
        assert false; // Should be unreachable
    }

    @Test
    public void testToArray() {

        CreateAccountsResultBatch batch = new CreateAccountsResultBatch(dummyStream.position(0));
        assertEquals(batch.getLenght(), 2);

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

        var batch = new CreateAccountsResultBatch(invalidBuffer);
        assert batch == null; // Should be unreachable
    }

    @Test
    public void testBufferLen() {
        var batch = new CreateAccountsResultBatch(dummyStream.position(0));
        assertEquals(dummyStream.capacity(), batch.getBufferLen());
    }

    private static void assertResults(CreateAccountsResult result1, CreateAccountsResult result2) {
        assertEquals(result1.index, result2.index);
        assertEquals(result1.result, result2.result);
    }
}
