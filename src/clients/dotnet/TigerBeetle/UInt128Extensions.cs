using System;
using System.Numerics;
using System.Runtime.InteropServices;

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
            return new Guid(new ReadOnlySpan<byte>(&value, SIZE));
        }
    }

    public static UInt128 ToUInt128(this Guid value)
    {
        unsafe
        {
            UInt128 ret = UInt128.Zero;

            // Passing a fixed 16-byte span, there's no possibility
            // of returning false.
            _ = value.TryWriteBytes(new Span<byte>(&ret, SIZE));

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
