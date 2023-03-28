package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import java.nio.ByteBuffer;
import org.junit.Test;

public class RequestMemcpyTest {

    @Test(expected = AssertionError.class)
    public void testMemcpyNullBuffer() {
        Request.memcpy(null);
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testMemcpyNonDirectBuffer() {
        var buffer = ByteBuffer.allocate(10);

        @SuppressWarnings("unused")
        var copy = Request.memcpy(buffer);
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testMemcpyEmptyBuffer() {
        var buffer = ByteBuffer.allocateDirect(0);

        @SuppressWarnings("unused")
        var copy = Request.memcpy(buffer);
        assert false;
    }

    @Test
    public void testMemcpy() {
        var buffer = ByteBuffer.allocateDirect(4);
        buffer.putInt(0, 42);

        var copy = Request.memcpy(buffer);

        assertEquals(buffer.capacity(), copy.capacity());
        assertEquals(buffer.getInt(0), copy.getInt(0));

        // Changes to the original buffer should not reflect on the copy
        buffer.putInt(0, 99);
        assertNotEquals(buffer.getInt(0), copy.getInt(0));
    }
}
