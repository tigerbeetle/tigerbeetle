package com.tigerbeetle;

import java.security.SecureRandom;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.UUID;

public enum UInt128 {

    LeastSignificant,
    MostSignificant;

    public static final int SIZE = 16;

    private static final BigInteger MOST_SIGNIFICANT_MASK = BigInteger.ONE.shiftLeft(64);
    private static final BigInteger LEAST_SIGNIFICANT_MASK = BigInteger.valueOf(Long.MAX_VALUE);

    /**
     * Gets the partial 64-bit representation of a 128-bit unsigned integer.
     *
     * @param bytes an array of 16 bytes representing the 128-bit value.
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be
     *        retrieved.
     * @return a {@code long} representing the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     *
     * @throws NullPointerException if {@code bytes} is null.
     * @throws IllegalArgumentException if {@code bytes} is not 16 bytes long.
     */
    public static long asLong(final byte[] bytes, final UInt128 part) {
        Objects.requireNonNull(bytes, "Bytes cannot be null");

        if (bytes.length != UInt128.SIZE)
            throw new IllegalArgumentException("Bytes must be 16 bytes long");

        var buffer = ByteBuffer.wrap(bytes).order(Batch.BYTE_ORDER).position(0);
        if (part == UInt128.MostSignificant)
            buffer.position(Long.BYTES);
        return buffer.getLong();
    }

    /**
     * Gets an array of 16 bytes representing the 128-bit value.
     *
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @param mostSignificant a {@code long} representing the last 8 bytes of the 128-bit value.
     * @return an array of 16 bytes representing the 128-bit value.
     */
    public static byte[] asBytes(final long leastSignificant, final long mostSignificant) {
        byte[] bytes = new byte[UInt128.SIZE];

        if (leastSignificant != 0 || mostSignificant != 0) {
            var buffer = ByteBuffer.wrap(bytes).order(Batch.BYTE_ORDER);
            buffer.putLong(leastSignificant);
            buffer.putLong(mostSignificant);
        }

        return bytes;
    }

    /**
     * Gets an array of 16 bytes representing the 128-bit value.
     *
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @return an array of 16 bytes representing the 128-bit value.
     */
    public static byte[] asBytes(final long leastSignificant) {
        return asBytes(leastSignificant, 0);
    }

    /**
     * Gets an array of 16 bytes representing the UUID.
     *
     * @param uuid a {@link java.util.UUID}
     * @return an array of 16 bytes representing the 128-bit value.
     *
     * @throws NullPointerException if {@code uuid} is null.
     */
    public static byte[] asBytes(final UUID uuid) {
        Objects.requireNonNull(uuid, "Uuid cannot be null");
        return asBytes(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
    }

    /**
     * Gets a {@link java.util.UUID} representing a 128-bit value.
     *
     * @param bytes an array of 16 bytes representing the 128-bit value.
     * @return a {@link java.util.UUID}.
     *
     * @throws NullPointerException if {@code bytes} is null.
     * @throws IllegalArgumentException if {@code bytes} is not 16 bytes long.
     */
    public static UUID asUUID(final byte[] bytes) {
        final long leastSignificant = asLong(bytes, UInt128.LeastSignificant);
        final long mostSignificant = asLong(bytes, UInt128.MostSignificant);
        return new UUID(mostSignificant, leastSignificant);
    }

    /**
     * Gets a {@link java.math.BigInteger} representing a 128-bit unsigned integer.
     *
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @param mostSignificant a {@code long} representing the last 8 bytes of the 128-bit value.
     * @return a {@link java.math.BigInteger}.
     */
    public static BigInteger asBigInteger(final long leastSignificant, final long mostSignificant) {
        if (leastSignificant == 0 && mostSignificant == 0) {
            return BigInteger.ZERO;
        }

        var bigintMsb = BigInteger.valueOf(mostSignificant);
        var bigintLsb = BigInteger.valueOf(leastSignificant);

        if (bigintMsb.signum() < 0) {
            bigintMsb = bigintMsb.add(MOST_SIGNIFICANT_MASK);
        }
        if (bigintLsb.signum() < 0) {
            bigintLsb = bigintLsb.add(MOST_SIGNIFICANT_MASK);
        }

        return bigintLsb.add(bigintMsb.multiply(MOST_SIGNIFICANT_MASK));
    }

    /**
     * Gets a {@link java.math.BigInteger} representing a 128-bit unsigned integer.
     *
     * @param bytes an array of 16 bytes representing the 128-bit value.
     * @return a {@code java.math.BigInteger}.
     *
     * @throws NullPointerException if {@code bytes} is null.
     * @throws IllegalArgumentException if {@code bytes} is not 16 bytes long.
     */
    public static BigInteger asBigInteger(final byte[] bytes) {
        Objects.requireNonNull(bytes, "Bytes cannot be null");

        if (bytes.length != UInt128.SIZE)
            throw new IllegalArgumentException("Bytes must be 16 bytes long");

        final var buffer = ByteBuffer.wrap(bytes).order(Batch.BYTE_ORDER).position(0);
        return asBigInteger(buffer.getLong(), buffer.getLong());
    }

    /**
     * Gets an array of 16 bytes representing the 128-bit unsigned integer.
     *
     * @param value a {@link java.math.BigDecimal}
     * @return an array of 16 bytes representing the 128-bit unsigned value.
     *
     * @throws NullPointerException if {@code value} is null.
     * @throws IllegalArgumentException if {@code value} is negative.
     */
    public static byte[] asBytes(final BigInteger value) {
        Objects.requireNonNull(value, "Value cannot be null");
        if (value.signum() < 0)
            throw new IllegalArgumentException("Value cannot be negative");

        if (BigInteger.ZERO.equals(value))
            return new byte[SIZE];

        final var parts = value.divideAndRemainder(MOST_SIGNIFICANT_MASK);
        var bigintMsb = parts[0];
        var bigintLsb = parts[1];

        if (LEAST_SIGNIFICANT_MASK.compareTo(bigintMsb) < 0) {
            bigintMsb = bigintMsb.subtract(MOST_SIGNIFICANT_MASK);
        }

        if (LEAST_SIGNIFICANT_MASK.compareTo(bigintLsb) < 0) {
            bigintLsb = bigintLsb.subtract(MOST_SIGNIFICANT_MASK);
        }

        return asBytes(bigintLsb.longValueExact(), bigintMsb.longValueExact());
    }

    private static long idLastTimestamp = 0L;
    private static final byte[] idLastRandom = new byte[10];
    private static final SecureRandom idSecureRandom = new SecureRandom();

    /**
     * Generates a Universally Unique Binary Sortable Identifier as 16 bytes of a 128-bit value.
     *
     * The ID() function is thread-safe, the bytes returned are stored in little endian, and the
     * unsigned 128-bit value increases monotonically. The algorithm is based on
     * <a href="https://github.com/ulid/spec">ULID</a> but is adjusted for u128-LE interpretation.
     *
     * @throws ArithmeticException if the random monotonic bits in the same millisecond overflows.
     * @return An array of 16 bytes representing an unsigned 128-bit value in little endian.
     */
    public static byte[] id() {
        long randomLo;
        short randomHi;
        long timestamp = System.currentTimeMillis();

        // Only modify the static variables in the synchronized block.
        synchronized (idSecureRandom) {
            // Ensure timestamp is monotonic. If it advances forward, also generate a new random.
            if (timestamp <= idLastTimestamp) {
                timestamp = idLastTimestamp;
            } else {
                idLastTimestamp = timestamp;
                idSecureRandom.nextBytes(idLastRandom);
            }

            var random = ByteBuffer.wrap(idLastRandom).order(ByteOrder.nativeOrder());
            randomLo = random.getLong();
            randomHi = random.getShort();

            // Increment the u80 stored in idLastRandom using a u64 increment then u16 increment.
            // Throws an exception if the entire u80 represented with both overflows.
            // In Java, all arithmetic wraps around on overflow by default so check for zero.
            randomLo += 1;
            if (randomLo == 0) {
                randomHi += 1;
                if (randomHi == 0) {
                    throw new ArithmeticException("Random bits overflow on monotonic increment");
                }
            }

            // Write back the incremented random.
            random.flip();
            random.putLong(randomLo);
            random.putShort(randomHi);
        }

        var buffer = ByteBuffer.allocate(UInt128.SIZE).order(Batch.BYTE_ORDER);
        buffer.putLong(randomLo);
        buffer.putShort(randomHi);
        buffer.putShort((short) timestamp); // timestamp lo
        buffer.putInt((int) (timestamp >> 16)); // timestamp hi
        return buffer.array();
    }
}
