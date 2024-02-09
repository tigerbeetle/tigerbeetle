package com.tigerbeetle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertSame;
import java.util.concurrent.CountDownLatch;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.junit.Test;

public class UInt128Test {

    // bytes representing a pair of longs (100, 1000):
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
