using System;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.InteropServices;

namespace TigerBeetle
{
    [StructLayout(LayoutKind.Sequential, Size = SIZE)]
    public struct UInt128 : IEquatable<UInt128>
    {
        public const int SIZE = 16;

        public static readonly UInt128 Zero = new();

        private readonly ulong _0;
        private readonly ulong _1;

        public UInt128(ReadOnlySpan<byte> bytes)
        {
            if (bytes.Length != SIZE) throw new ArgumentException(nameof(bytes));

            var values = MemoryMarshal.Cast<byte, ulong>(bytes);
            _0 = values[0];
            _1 = values[1];
        }

        public UInt128(Guid guid)
        {
            _0 = 0LU;
            _1 = 0LU;

            if (!TryWriteBytes(guid)) throw new ArgumentException(nameof(guid));
        }

        public UInt128(BigInteger bigInteger)
        {
            _0 = 0LU;
            _1 = 0LU;

            if (!TryWriteBytes(bigInteger)) throw new ArgumentException(nameof(bigInteger));
        }

        public UInt128(long a, long b = 0L)
        {
            unchecked
            {
                _0 = (ulong)a;
                _1 = (ulong)b;
            }
        }

        public UInt128(ulong a, ulong b = 0LU)
        {
            _0 = a;
            _1 = b;
        }

        public Guid ToGuid()
        {
            unsafe
            {
                fixed (void* ptr = &this)
                {
                    return new Guid(new ReadOnlySpan<byte>(ptr, SIZE));
                }
            }
        }

        public byte[] ToArray()
        {
            unsafe
            {
                fixed (void* ptr = &this)
                {
                    var span = new ReadOnlySpan<byte>(ptr, SIZE);
                    return span.ToArray();
                }
            }
        }

        public BigInteger ToBigInteger()
        {
            unsafe
            {
                fixed (void* ptr = &this)
                {
                    var span = new ReadOnlySpan<byte>(ptr, SIZE);
                    return new BigInteger(span, isUnsigned: true, isBigEndian: false);
                }
            }
        }

        public (long, long) ToInt64()
        {
            unchecked
            {
                return ((long)_0, (long)_1);
            }
        }

        public (ulong, ulong) ToUInt64() => (_0, _1);

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            return obj switch
            {
                UInt128 _uint128 => Equals(_uint128),
                Guid _guid => Equals(_guid),
                BigInteger _bigInteger => Equals(_bigInteger),
                byte[] array => Equals(array),
                long _long => Equals((UInt128)_long),
                ulong _ulong => Equals((UInt128)_ulong),
                int _int => Equals((UInt128)_int),
                uint _uint => Equals((UInt128)_uint),
                _ => false,
            };
        }

        public bool Equals(byte[] array)
        {
            return array != null && array.Length == SIZE && this.Equals(new UInt128(array));
        }

        public bool Equals(Guid guid)
        {
            var other = new UInt128();
            return other.TryWriteBytes(guid) && this.Equals(other);
        }

        public bool Equals(BigInteger bigInteger)
        {
            var other = new UInt128();
            return other.TryWriteBytes(bigInteger) && this.Equals(other);
        }

        public bool Equals(UInt128 other) => _0 == other._0 && _1 == other._1;

        public override int GetHashCode() => HashCode.Combine(_0, _1);

        public override string ToString() => ToBigInteger().ToString();

        private bool TryWriteBytes(Guid guid)
        {
            unsafe
            {
                fixed (void* ptr = &this)
                {
                    var span = new Span<byte>(ptr, SIZE);
                    return guid.TryWriteBytes(span);
                }
            }
        }

        private bool TryWriteBytes(BigInteger bigInteger)
        {
            unsafe
            {
                fixed (void* ptr = &this)
                {
                    var span = new Span<byte>(ptr, SIZE);
                    return bigInteger.TryWriteBytes(span, out int _, isUnsigned: true, isBigEndian: false);
                }
            }
        }

        public static bool operator ==(UInt128 left, UInt128 right) => left.Equals(right);

        public static bool operator !=(UInt128 left, UInt128 right) => !left.Equals(right);

        public static implicit operator UInt128(Guid guid) => new(guid);

        public static implicit operator Guid(UInt128 value) => value.ToGuid();

        public static implicit operator UInt128(BigInteger bigInteger) => new(bigInteger);

        public static implicit operator BigInteger(UInt128 value) => value.ToBigInteger();

        public static implicit operator UInt128(byte[] array) => new(array);

        public static implicit operator byte[](UInt128 value) => value.ToArray();

        public static implicit operator UInt128(long value)
        {
            unchecked
            {
                return new UInt128((ulong)value, 0LU);
            }
        }

        public static implicit operator UInt128(ulong value) => new(value, 0LU);

        public static implicit operator UInt128(int value)
        {
            unchecked
            {
                return new UInt128((ulong)value, 0LU);
            }
        }

        public static implicit operator UInt128(uint value) => new((ulong)value, 0LU);
    }
}
