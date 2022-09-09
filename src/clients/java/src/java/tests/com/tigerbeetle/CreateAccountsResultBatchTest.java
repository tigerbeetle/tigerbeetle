package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;

/**
 * Asserts the memory interpretation from/to a binary stream
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
    public void testGet() throws RequestException {

        CreateAccountsResultBatch batch = new CreateAccountsResultBatch(dummyStream.position(0));
        assertEquals(batch.getLenght(), 2);

        CreateAccountsResult getResult1 = batch.get(0);
        assertNotNull(getResult1);
        assertResults(result1, getResult1);

        CreateAccountsResult getResult2 = batch.get(1);
        assertNotNull(getResult2);
        assertResults(result2, getResult2);
    }

    @Test
    public void testToArray() throws RequestException {

        CreateAccountsResultBatch batch = new CreateAccountsResultBatch(dummyStream.position(0));
        assertEquals(batch.getLenght(), 2);

        CreateAccountsResult[] array = batch.toArray();
        assertEquals(array.length, 2);
        assertResults(result1, array[0]);
        assertResults(result2, array[1]);
    }

    private static void assertResults(CreateAccountsResult result1, CreateAccountsResult result2) {
        assertEquals(result1.index, result2.index);
        assertEquals(result1.result, result2.result);
    }     
}
