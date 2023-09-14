package com.tigerbeetle;

import java.math.BigInteger;
import java.nio.ByteBuffer;
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
     * @return a {@code long} representing the the first 8 bytes of the 128-bit value if
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
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
     * @param mostSignificant a {@code long} representing the the last 8 bytes of the 128-bit value.
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
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
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
     * @param leastSignificant a {@code long} representing the the first 8 bytes of the 128-bit
     *        value.
     * @param mostSignificant a {@code long} representing the the last 8 bytes of the 128-bit value.
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
     * @return an array of 16 bytes representing the 128-bit value.
     *
     * @throws NullPointerException if {@code value} is null.
     */
    public static byte[] asBytes(final BigInteger value) {
        Objects.requireNonNull(value, "Value cannot be null");
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


}
