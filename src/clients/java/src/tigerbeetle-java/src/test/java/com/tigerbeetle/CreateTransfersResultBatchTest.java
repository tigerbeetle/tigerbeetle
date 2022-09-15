package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;

/**
 * Asserts the memory interpretation from/to a binary stream
 */
public class CreateTransfersResultBatchTest {

    private static final CreateTransfersResult result1;
    private static final CreateTransfersResult result2;
    private static final ByteBuffer dummyStream;

    static {

        result1 = new CreateTransfersResult(0, CreateTransferResult.Ok);
        result2 = new CreateTransfersResult(1, CreateTransferResult.ExceedsDebits);

        // Mimic the the binnary response
        dummyStream = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        dummyStream.putInt(0).putInt(0); // Item 0 - OK
        dummyStream.putInt(1).putInt(36); // Item 1 - ExceedsDebits
    }

    @Test
    public void testGet() throws RequestException {

        CreateTransfersResultBatch batch = new CreateTransfersResultBatch(dummyStream.position(0));
        assertEquals(batch.getLenght(), 2);

        CreateTransfersResult getResult1 = batch.get(0);
        assertNotNull(getResult1);
        assertResults(result1, getResult1);

        CreateTransfersResult getResult2 = batch.get(1);
        assertNotNull(getResult2);
        assertResults(result2, getResult2);
    }

    @Test
    public void testToArray() throws RequestException {

        CreateTransfersResultBatch batch = new CreateTransfersResultBatch(dummyStream.position(0));
        assertEquals(batch.getLenght(), 2);

        CreateTransfersResult[] array = batch.toArray();
        assertEquals(array.length, 2);
        assertResults(result1, array[0]);
        assertResults(result2, array[1]);
    }

    private static void assertResults(CreateTransfersResult result1,
            CreateTransfersResult result2) {
        assertEquals(result1.index, result2.index);
        assertEquals(result1.result, result2.result);
    }
}
