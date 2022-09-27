package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.UUID;

public enum UInt128 {

    LeastSignificant,
    MostSignificant;

    public static final int SIZE = 16;

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

    public static byte[] asBytes(final long leastSignificant, final long mostSignificant) {
        byte[] bytes = new byte[UInt128.SIZE];

        if (leastSignificant != 0 || mostSignificant != 0) {
            var buffer = ByteBuffer.wrap(bytes).order(Batch.BYTE_ORDER);
            buffer.putLong(leastSignificant);
            buffer.putLong(mostSignificant);
        }

        return bytes;
    }

    public static byte[] asBytes(final UUID uuid) {
        if (uuid == null)
            throw new NullPointerException("Uuid cannot be null");

        return asBytes(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
    }

    public static UUID asUUID(final byte[] bytes) {
        final long leastSignificant = asLong(bytes, UInt128.LeastSignificant);
        final long mostSignificant = asLong(bytes, UInt128.MostSignificant);
        return new UUID(mostSignificant, leastSignificant);
    }
}
