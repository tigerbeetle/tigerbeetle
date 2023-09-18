using System;
using System.Numerics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace TigerBeetle
{
	/// <summary>
	/// Conversion functions between UInt128 and commonly used types such as Guid, BigInteger and byte[].
	/// </summary>
    public static class UInt128Extensions
    {
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

// DotNet 7.0 already introduces UInt128.
// We keep the bare minimum implementation for compatibility reasons.
#if !NET7_0_OR_GREATER
	[StructLayout(LayoutKind.Sequential, Size = UInt128Extensions.SIZE)]
    public struct UInt128 : IEquatable<UInt128>
    {
        public const int SIZE = UInt128Extensions.SIZE;

        public static readonly UInt128 Zero = new();

        private readonly ulong _0;
        private readonly ulong _1;
        public UInt128(ulong mostSignificantBytes, ulong leastSignificantBytes = 0L)
        {
            _0 = leastSignificantBytes;
            _1 = mostSignificantBytes;
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
			return obj switch
			{
				UInt128 other => Equals(other),
				_ => false,
			};
        }

        public bool Equals(UInt128 other) => _0 == other._0 && _1 == other._1;

        public override int GetHashCode() => HashCode.Combine(_0, _1);

        public override string ToString() => this.ToBigInteger().ToString();

        public static bool operator ==(UInt128 left, UInt128 right) => left.Equals(right);

        public static bool operator !=(UInt128 left, UInt128 right) => !left.Equals(right);

		public static implicit operator UInt128(ushort value) => new UInt128(0, value);

		public static implicit operator UInt128(uint value) => new UInt128(0, value);

		public static implicit operator UInt128(ulong value) => new UInt128(0, value);

		public static implicit operator UInt128(nuint value) => new UInt128(0, value);
    }
#endif
}
