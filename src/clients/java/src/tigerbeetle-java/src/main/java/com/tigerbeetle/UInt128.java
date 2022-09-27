package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

public enum UInt128 {

    LeastSignificant,
    MostSignificant;

    public static final int SIZE = 16;

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

        if (bytes == null)
            throw new NullPointerException("Bytes cannot be null");

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
     * Gets an array of 16 bytes representing the UUID.
     * 
     * @param uuid a {@link java.util.UUID}
     * @return an array of 16 bytes representing the 128-bit value.
     */    
    public static byte[] asBytes(final UUID uuid) {
        if (uuid == null)
            throw new NullPointerException("Uuid cannot be null");

        return asBytes(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
    }

    /**
     * Gets a {@link java.util.UUID} representing a 128-bit value.
     * 
     * @param bytes an array of 16 bytes representing the 128-bit value.
     * @return a {@link java.util.UUID}.
     */    
    public static UUID asUUID(final byte[] bytes) {
        final long leastSignificant = asLong(bytes, UInt128.LeastSignificant);
        final long mostSignificant = asLong(bytes, UInt128.MostSignificant);
        return new UUID(mostSignificant, leastSignificant);
    }
}
