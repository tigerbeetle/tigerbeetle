package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.util.UUID;
import org.junit.Test;

public class UInt128Test {

    // pair (100, 1000)
    final static byte[] bytes = new byte[] {100, 0, 0, 0, 0, 0, 0, 0, -24, 3, 0, 0, 0, 0, 0, 0};

    @Test(expected = NullPointerException.class)
    public void testAsLongNull() {

        @SuppressWarnings("unused")
        var nop = UInt128.asLong(null, UInt128.LeastSignificant);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAsLongInvalid() {

        byte[] bytes = new byte[] {1, 2, 3, 4, 5, 6};
        @SuppressWarnings("unused")
        var nop = UInt128.asLong(bytes, UInt128.LeastSignificant);
        assert false;
    }

    @Test
    public void testAsLong() {

        var ls = UInt128.asLong(bytes, UInt128.LeastSignificant);
        var ms = UInt128.asLong(bytes, UInt128.MostSignificant);
        assertEquals(100L, ls);
        assertEquals(1000L, ms);

        byte[] reverse = UInt128.asBytes(100, 1000);
        assertArrayEquals(bytes, reverse);
    }

    @Test
    public void testAsBytes() {

        byte[] reverse = UInt128.asBytes(100, 1000);
        assertArrayEquals(bytes, reverse);
    }

    @Test
    public void testAsBytesZero() {

        byte[] reverse = UInt128.asBytes(0, 0);
        assertArrayEquals(new byte[16], reverse);
    }

    @Test(expected = NullPointerException.class)
    public void testAsBytesUUIDNull() {

        UUID uuid = null;
        @SuppressWarnings("unused")
        var nop = UInt128.asBytes(uuid);
        assert false;
    }

    @Test
    public void testAsBytesUUID() {
        var uuid = new UUID(1000, 100);
        byte[] reverse = UInt128.asBytes(uuid);
        assertArrayEquals(bytes, reverse);
    }

    @Test
    public void testAsUUID() {
        var uuid = UInt128.asUUID(bytes);
        assertEquals(new UUID(1000, 100), uuid);
    }

    @Test(expected = NullPointerException.class)
    public void testAsUUIDNull() {

        @SuppressWarnings("unused")
        var nop = UInt128.asUUID(null);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAsUUIDInvalid() {

        byte[] bytes = new byte[] {1, 2, 3, 4, 5, 6};
        @SuppressWarnings("unused")
        var nop = UInt128.asUUID(bytes);
        assert false;
    }


}
