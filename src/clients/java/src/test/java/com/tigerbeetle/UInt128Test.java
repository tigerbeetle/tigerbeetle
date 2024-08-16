package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertSame;
import java.util.concurrent.CountDownLatch;
import java.math.BigInteger;
import java.util.UUID;
import org.junit.Test;

public class UInt128Test {

    // bytes representing a pair of longs (100, 1000):
    final static byte[] bytes = new byte[] {100, 0, 0, 0, 0, 0, 0, 0, -24, 3, 0, 0, 0, 0, 0, 0};

    /// Consistency of U128 across Zig and the language clients.
    /// It must be kept in sync with all platforms.
    @Test
    public void consistencyTest() {
        // Decimal representation:
        final long upper = Long.parseUnsignedLong("16637956065805325009");
        final long lower = Long.parseUnsignedLong("14033694389904319137");
        final var u128 = UInt128.asBigInteger(lower, upper);
        assertEquals("306916117455534265709072286748445483681", u128.toString());

        // Binary representation:
        final byte[] binary = new byte[] {(byte) 0xa1, (byte) 0xa2, (byte) 0xa3, (byte) 0xa4,
                (byte) 0xb1, (byte) 0xb2, (byte) 0xc1, (byte) 0xc2, (byte) 0xd1, (byte) 0xd2,
                (byte) 0xe1, (byte) 0xe2, (byte) 0xe3, (byte) 0xe4, (byte) 0xe5, (byte) 0xe6};
        final var bytes = UInt128.asBytes(lower, upper);
        assertArrayEquals(binary, bytes);

        // UUID representation:
        final var guid = UUID.fromString("a1a2a3a4-b1b2-c1c2-d1d2-e1e2e3e4e5e6");
        assertEquals(guid, UInt128.asUUID(bytes));
        assertArrayEquals(bytes, UInt128.asBytes(guid));
        assertEquals(u128, UInt128.asBigInteger(UInt128.asBytes(guid)));
    }

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
    public void testAsBytesFromSingleLong() {

        byte[] singleLong = UInt128.asBytes(100L);

        assertEquals(100L, UInt128.asLong(singleLong, UInt128.LeastSignificant));
        assertEquals(0L, UInt128.asLong(singleLong, UInt128.MostSignificant));
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
        var uuid = UInt128.asUUID(100, 1000);
        byte[] reverse = UInt128.asBytes(uuid);
        assertArrayEquals(bytes, reverse);
    }

    @Test
    public void testAsUUID() {
        var uuid = UInt128.asUUID(bytes);
        assertEquals(UInt128.asUUID(100, 1000), uuid);
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

    @Test(expected = NullPointerException.class)
    public void testAsBytesBigIntegerNull() {

        BigInteger bigint = null;
        @SuppressWarnings("unused")
        var nop = UInt128.asBytes(bigint);
        assert false;
    }

    @Test
    public void testAsBigIntegerFromLong() {
        var bigint = UInt128.asBigInteger(100, 1000);

        // Bigint representation of a pair of longs (100, 1000)
        var reverse = BigInteger.valueOf(100)
                .add(BigInteger.valueOf(1000).multiply(BigInteger.ONE.shiftLeft(64)));

        assertEquals(reverse, bigint);
        assertArrayEquals(bytes, UInt128.asBytes(bigint));
    }

    @Test
    public void testAsBigIntegerFromBytes() {
        var bigint = UInt128.asBigInteger(bytes);

        assertEquals(UInt128.asBigInteger(100, 1000), bigint);
        assertArrayEquals(bytes, UInt128.asBytes(bigint));
    }

    @Test(expected = NullPointerException.class)
    public void testAsBigIntegerNull() {

        @SuppressWarnings("unused")
        var nop = UInt128.asBigInteger(null);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAsBigIntegerInvalid() {

        byte[] bytes = new byte[] {1, 2, 3, 4, 5, 6};
        @SuppressWarnings("unused")
        var nop = UInt128.asBigInteger(bytes);
        assert false;
    }

    @Test
    public void testAsBigIntegerUnsigned() {

        // @bitCast(u128, [2]i64{ -100, -1000 }) == 340282366920938445035077277795926146972
        final var expected = new BigInteger("340282366920938445035077277795926146972");

        assertEquals(expected, UInt128.asBigInteger(-100, -1000));
        assertArrayEquals(UInt128.asBytes(expected), UInt128.asBytes(-100, -1000));
    }

    @Test
    public void testAsBigIntegerZero() {
        assertSame(BigInteger.ZERO, UInt128.asBigInteger(0, 0));
        assertSame(BigInteger.ZERO, UInt128.asBigInteger(new byte[16]));
        assertArrayEquals(new byte[16], UInt128.asBytes(BigInteger.ZERO));
    }

    @Test
    public void testID() throws Exception {
        {
            // Generate IDs, sleeping for ~1ms occasionally to test intra-millisecond monotonicity.
            var idA = UInt128.asBigInteger(UInt128.id());
            for (int i = 0; i < 1_000_000; i++) {
                if (i % 10_000 == 0) {
                    Thread.sleep(1);
                }

                var idB = UInt128.asBigInteger(UInt128.id());
                assertTrue(idB.compareTo(idA) > 0);

                // Use the generated ID as the new reference point for the next loop.
                idA = idB;
            }
        }

        final var threadExceptions = new Exception[100];
        final var latchStart = new CountDownLatch(threadExceptions.length);
        final var latchFinish = new CountDownLatch(threadExceptions.length);

        for (int i = 0; i < threadExceptions.length; i++) {
            final int threadIndex = i;
            new Thread(() -> {
                try {
                    // Wait for all threads to spawn before starting.
                    latchStart.countDown();
                    latchStart.await();

                    // Same as serial test above, but with smaller bounds.
                    var idA = UInt128.asBigInteger(UInt128.id());
                    for (int j = 0; j < 10_000; j++) {
                        if (j % 1000 == 0) {
                            Thread.sleep(1);
                        }

                        var idB = UInt128.asBigInteger(UInt128.id());
                        assertTrue(idB.compareTo(idA) > 0);
                        idA = idB;
                    }

                } catch (Exception e) {
                    threadExceptions[threadIndex] = e; // Propagate exceptions to main thread.
                } finally {
                    latchFinish.countDown(); // Make sure to unblock the main thread.
                }
            }).start();
        }

        latchFinish.await();
        for (var exception : threadExceptions) {
            if (exception != null)
                throw exception;
        }
    }
}
