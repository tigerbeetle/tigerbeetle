package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class UInt128 {

    public enum Bytes {
        LeastSignificant,
        MostSignificant;

        public static final int SIZE = 16;
    }

    private UInt128() {}

    public static long getLong(final byte[] bytes, final Bytes part) {

        if (bytes == null)
            throw new NullPointerException("Bytes cannot be null");

        if (bytes.length != Bytes.SIZE)
            throw new IllegalArgumentException("Bytes must be 16 bytes long");


        var buffer = ByteBuffer.wrap(bytes).order(Batch.BYTE_ORDER).position(0);
        if (part == Bytes.MostSignificant)
            buffer.position(Long.BYTES);
        return buffer.getLong();
    }

    public static byte[] toBytes(final long leastSignificant, final long mostSignificant) {
        byte[] bytes = new byte[Bytes.SIZE];

        if (leastSignificant != 0 || mostSignificant != 0) {
            var buffer = ByteBuffer.wrap(bytes).order(Batch.BYTE_ORDER);
            buffer.putLong(leastSignificant);
            buffer.putLong(mostSignificant);
        }

        return bytes;
    }

    public static byte[] fromUUID(final UUID uuid) {
        return toBytes(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
    }

    public static UUID toUUID(final byte[] bytes) {
        final long leastSignificant = getLong(bytes, Bytes.LeastSignificant);
        final long mostSignificant = getLong(bytes, Bytes.MostSignificant);
        return new UUID(mostSignificant, leastSignificant);
    }

}
