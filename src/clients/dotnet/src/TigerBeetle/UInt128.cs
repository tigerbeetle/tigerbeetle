﻿using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace TigerBeetle
{
    [StructLayout(LayoutKind.Explicit, Size = SIZE)]
    public struct UInt128 : IEquatable<UInt128>
    {
        #region Fields

        public const int SIZE = 16;

        public static readonly UInt128 Zero = new UInt128();

        [FieldOffset(0)]
        private readonly ulong _0;

        [FieldOffset(8)]
        private readonly ulong _1;

        #endregion Fields

        #region Constructor

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

            FromGuid(guid);
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

        #endregion Constructor

        #region Methods

        public Guid ToGuid() => new(AsReadOnlySpan<byte>());

        public (long, long) ToInt64()
        {
            unchecked
            {
                return ((long)_0, (long)_1);
            }
        }

        public (ulong, ulong) ToUInt64() => (_0, _1);

        internal Span<T> AsSpan<T>()
        {
            unsafe
            {
                fixed (void* ptr = &this)
                {
                    return new Span<T>(ptr, SIZE / Marshal.SizeOf<T>());
                }
            }
        }

        internal void FromGuid(Guid guid) => _ = guid.TryWriteBytes(AsSpan<byte>());

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            return obj switch
            {
                UInt128 _uint128 => Equals(_uint128),
                Guid _guid => Equals((UInt128)_guid),
                long _long => Equals((UInt128)_long),
                ulong _ulong => Equals((UInt128)_ulong),
                int _int => Equals((UInt128)_int),
                uint _uint => Equals((UInt128)_uint),
                _ => false,
            };
        }

        public bool Equals(UInt128 other) => _0 == other._0 && _1 == other._1;

        public override int GetHashCode() => HashCode.Combine(_0, _1);
        
        public override string ToString() => ToGuid().ToString();
        
        public static bool operator ==(UInt128 left, UInt128 right) => left.Equals(right);
        
        public static bool operator !=(UInt128 left, UInt128 right) => !left.Equals(right);

        public static implicit operator Guid(UInt128 value) => value.ToGuid();
        
        public static implicit operator UInt128(Guid guid) => new UInt128(guid);
        
        public static implicit operator UInt128(long value)
        {
            unchecked
            {
                return new UInt128((ulong)value, 0LU);
            }
        }
        
        public static implicit operator UInt128(ulong value) => new UInt128(value, 0LU);

        public static implicit operator UInt128(int value)
        {
            unchecked
            {
                return new UInt128((ulong)value, 0LU);
            }
        } 
        
        public static implicit operator UInt128(uint value) => new UInt128((ulong)value, 0LU);

        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            unsafe
            {
                fixed (void* ptr = &this)
                {
                    return new ReadOnlySpan<byte>(ptr, SIZE);
                }
            }
        }

        internal ReadOnlySpan<T> AsReadOnlySpan<T>()
            where T : struct
        {
            return MemoryMarshal.Cast<byte, T>(AsReadOnlySpan());
        }

        #endregion Methods
    }
}
