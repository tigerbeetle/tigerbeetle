using System;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace TigerBeetle;

/// <summary>
/// Conversion functions between UInt128 and commonly used types such as Guid, BigInteger and byte[].
/// </summary>
public static class UInt128Extensions
{
    /// <summary>
    /// Unsafe representation of a tb_uint128_t used only internally
    /// for P/Invove in methods marked with the [DllImport] attribute.
    /// It's necessary only because P/Invoke's marshaller does not support System.UInt128.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = SIZE)]
    internal unsafe struct UnsafeU128
    {
        [FieldOffset(0)]
        fixed byte raw[SIZE];

        /// <summary>
        /// Reinterprets memory, casting the managed UInt128 directly to the unsafe representation.
        /// </summary>
        public static implicit operator UnsafeU128(UInt128 value) => *(UnsafeU128*)&value;
    }

    internal const int SIZE = 16;

    public static Guid ToGuid(this UInt128 value)
    {
        Span<byte> data = stackalloc byte[SIZE];
        MemoryMarshal.Write(data, in value);

        // The GUID layout is big endian.
        // This is important to preserve the string representation.
        return new Guid(
            BinaryPrimitives.ReadInt32LittleEndian(data[12..16]),
            BinaryPrimitives.ReadInt16LittleEndian(data[10..12]),
            BinaryPrimitives.ReadInt16LittleEndian(data[8..10]),
            data[7],
            data[6],
            data[5],
            data[4],
            data[3],
            data[2],
            data[1],
            data[0]);
    }

    public static UInt128 ToUInt128(this Guid value)
    {
        // Converting from big endian to little endian:
        Span<byte> data = stackalloc byte[SIZE];
        _ = value.TryWriteBytes(data, bigEndian: true, bytesWritten: out _);

        ulong upper = BinaryPrimitives.ReadUInt64BigEndian(data[0..8]);
        ulong lower = BinaryPrimitives.ReadUInt64BigEndian(data[8..16]);
        return new UInt128(upper, lower);
    }

    public static byte[] ToArray(this UInt128 value)
    {
        unsafe
        {
            var span = new ReadOnlySpan<byte>(&value, SIZE);
            return span.ToArray();
        }
    }

    public static UInt128 ToUInt128(this ReadOnlySpan<byte> memory)
    {
        if (memory.Length != SIZE) throw new ArgumentException(nameof(memory));

        unsafe
        {
            fixed (void* ptr = memory)
            {
                return *(UInt128*)ptr;
            }
        }
    }

    public static UInt128 ToUInt128(this byte[] array)
    {
        if (array == null) throw new ArgumentNullException(nameof(array));
        if (array.Length != SIZE) throw new ArgumentException(nameof(array));
        return new ReadOnlySpan<byte>(array, 0, SIZE).ToUInt128();
    }

    public static BigInteger ToBigInteger(this UInt128 value)
    {
        unsafe
        {
            return new BigInteger(new ReadOnlySpan<byte>(&value, SIZE), isUnsigned: true, isBigEndian: false);
        }
    }

    public static UInt128 ToUInt128(this BigInteger value)
    {
        unsafe
        {
            UInt128 ret = UInt128.Zero;
            if (!value.TryWriteBytes(new Span<byte>(&ret, SIZE), out int _, isUnsigned: true, isBigEndian: false))
            {
                throw new ArgumentOutOfRangeException();
            }

            return ret;
        }
    }
}

/// <summary>
/// Universally Unique and Binary-Sortable Identifiers as UInt128s based on
/// <a href="https://github.com/ulid/spec">ULID</a>
/// </summary>
public static class ID
{
    private static long idLastTimestamp = 0L;
    private static readonly byte[] idLastRandom = new byte[10];

    /// <summary>
    /// Generates a universally unique identifier as a UInt128.
    /// IDs are guaranteed to be monotonically increasing from the last.
    /// This function is thread-safe and monotonicity is sequentially consistent.
    /// </summary>
    public static UInt128 Create()
    {
        long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        ulong randomLo;
        ushort randomHi;

        lock (idLastRandom)
        {
            if (timestamp <= idLastTimestamp)
            {
                timestamp = idLastTimestamp;
            }
            else
            {
                idLastTimestamp = timestamp;
                RandomNumberGenerator.Fill(idLastRandom);
            }

            Span<byte> lastRandom = idLastRandom;
            randomLo = BitConverter.ToUInt64(lastRandom.Slice(0));
            randomHi = BitConverter.ToUInt16(lastRandom.Slice(8));

            // Increment the u80 stored in lastRandom using a u64 increment then u16 increment.
            // Throws an exception if the entire u80 represented with both overflows.
            // We rely on unsigned arithmetic wrapping on overflow by detecting for zero after inc.
            // Unsigned types wrap by default but can be overridden by compiler flag so be explicit.
            unchecked
            {
                randomLo += 1;
                if (randomLo == 0)
                {
                    randomHi += 1;
                    if (randomHi == 0)
                    {
                        throw new OverflowException("Random bits overflow on monotonic increment");
                    }
                }
            }

            BitConverter.TryWriteBytes(lastRandom.Slice(0), randomLo);
            BitConverter.TryWriteBytes(lastRandom.Slice(8), randomHi);
        }

        Span<byte> bytes = stackalloc byte[16];
        BitConverter.TryWriteBytes(bytes.Slice(0), randomLo);
        BitConverter.TryWriteBytes(bytes.Slice(8), randomHi);
        BitConverter.TryWriteBytes(bytes.Slice(10), (ushort)(timestamp));
        BitConverter.TryWriteBytes(bytes.Slice(12), (uint)(timestamp >> 16));
        return ((ReadOnlySpan<byte>)bytes).ToUInt128();
    }
}
