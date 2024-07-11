package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class QueryFilterTest {

    @Test
    public void testDefaultValues() {
        final var queryFilter = new QueryFilter();
        assertEquals(0L, queryFilter.getUserData128(UInt128.LeastSignificant));
        assertEquals(0L, queryFilter.getUserData128(UInt128.MostSignificant));
        assertEquals(0L, queryFilter.getUserData64());
        assertEquals(0, queryFilter.getUserData32());
        assertEquals(0, queryFilter.getLedger());
        assertEquals(0, queryFilter.getCode());
        assertEquals(0L, queryFilter.getTimestampMin());
        assertEquals(0L, queryFilter.getTimestampMax());
        assertEquals(0, queryFilter.getLimit());
        assertEquals(false, queryFilter.getReversed());
    }

    @Test
    public void testUserData128() {
        final var queryFilter = new QueryFilter();

        queryFilter.setUserData128(100, 200);
        assertEquals(100L, queryFilter.getUserData128(UInt128.LeastSignificant));
        assertEquals(200L, queryFilter.getUserData128(UInt128.MostSignificant));
    }

    @Test
    public void testUserData128Long() {
        final var queryFilter = new QueryFilter();

        queryFilter.setUserData128(100);
        assertEquals(100L, queryFilter.getUserData128(UInt128.LeastSignificant));
        assertEquals(0L, queryFilter.getUserData128(UInt128.MostSignificant));
    }

    @Test
    public void testUserData128AsBytes() {
        final var queryFilter = new QueryFilter();

        final var data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
        queryFilter.setUserData128(data);
        assertArrayEquals(data, queryFilter.getUserData128());
    }

    @Test
    public void testUserData128Null() {
        final var queryFilter = new QueryFilter();

        final byte[] data = null;
        queryFilter.setUserData128(data);

        assertArrayEquals(new byte[16], queryFilter.getUserData128());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserData128Invalid() {
        final var queryFilter = new QueryFilter();

        final var data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        queryFilter.setUserData128(data);
        assert false;
    }

    @Test
    public void testUserData64() {
        final var queryFilter = new QueryFilter();

        queryFilter.setUserData64(100L);
        assertEquals(100L, queryFilter.getUserData64());
    }

    @Test
    public void testUserData32() {
        final var queryFilter = new QueryFilter();

        queryFilter.setUserData32(10);
        assertEquals(10, queryFilter.getUserData32());
    }

    @Test
    public void testLedger() {
        final var queryFilter = new QueryFilter();

        queryFilter.setLedger(99);
        assertEquals(99, queryFilter.getLedger());
    }

    @Test
    public void testCode() {
        final var queryFilter = new QueryFilter();

        queryFilter.setCode(1);
        assertEquals(1, queryFilter.getCode());
    }

    @Test
    public void testTimestampMin() {
        final var queryFilter = new QueryFilter();

        queryFilter.setTimestampMin(100L);
        assertEquals(100, queryFilter.getTimestampMin());
    }

    @Test
    public void testTimestampMax() {
        final var queryFilter = new QueryFilter();

        queryFilter.setTimestampMax(100L);
        assertEquals(100, queryFilter.getTimestampMax());
    }

    @Test
    public void testLimit() {
        final var queryFilter = new QueryFilter();

        queryFilter.setLimit(30);
        assertEquals(30, queryFilter.getLimit());
    }

    @Test
    public void testFlags() {
        final var queryFilter = new QueryFilter();
        assertEquals(false, queryFilter.getReversed());
        queryFilter.setReversed(true);
        assertEquals(true, queryFilter.getReversed());
        queryFilter.setReversed(false);
        assertEquals(false, queryFilter.getReversed());
    }

    @Test
    public void testReserved() {
        final var queryFilter = new QueryFilterBatch(1);
        queryFilter.add();

        // Empty array:
        final var bytes = new byte[6];
        assertArrayEquals(new byte[6], queryFilter.getReserved());

        // Null == empty array:
        queryFilter.setReserved(null);

        for (byte i = 0; i < 6; i++) {
            bytes[i] = i;
        }
        queryFilter.setReserved(bytes);
        assertArrayEquals(bytes, queryFilter.getReserved());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReservedInvalid() {
        final var queryFilter = new QueryFilterBatch(1);
        queryFilter.add();
        queryFilter.setReserved(new byte[7]);
        assert false;
    }

}
