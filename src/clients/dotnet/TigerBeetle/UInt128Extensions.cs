using System;
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
        unsafe
        {
            var bytes = new Span<byte>(&value, SIZE);

            // The GUID type internal byte layout in dotnet is
            // 4 byte int (typically little endian but can be big endian depending on cpu)
            // 2 byte short (typically little endian but can be big endian depending on cpu)
            // 2 byte short (typically little endian but can be big endian depending on cpu)
            // 8 bytes stored as-is
            // We must use the bigEndian:true parameter to ensure the bytes are stored internally 
            // exactly as we have provided them, because 1. the inverse `ToUint128` constructs the
            // UInt128 by reading the bytes as-is 2. everything outside of dotnet e.g. TigerBeetle
            // Entity Framework handles the bytes as-is.
            return new Guid(bytes, bigEndian: true);
        }
    }

    public static UInt128 ToUInt128(this Guid value)
    {
        unsafe
        {
            UInt128 ret = UInt128.Zero;
            Span<byte> bytes = new Span<byte>(&ret, SIZE);

            // Passing a fixed 16-byte span, there's no possibility
            // of returning false.
            // `bigEndian: true` signals to write the bytes as they're stored
            // without reinterpreting them based on CPU endianness
            _ = value.TryWriteBytes(bytes, bigEndian: true, out _);

            return ret;
        }
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
