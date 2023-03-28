using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Globalization;
using System.Linq;
using System.Numerics;

namespace TigerBeetle.Tests
{
    [TestClass]
    public class UInt128Tests
    {
        [TestMethod]
        public void GuidConvertion()
        {
            Guid guid = Guid.Parse("A945C62A-4CC7-425B-B44A-893577632902");
            UInt128 value = guid;

            Assert.AreEqual(value, (UInt128)guid);
            Assert.AreEqual(guid, (Guid)value);
            Assert.IsTrue(value.Equals(guid));
        }

        [TestMethod]
        public void GuidMaxConvertion()
        {
            Guid guid = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff");
            _ = new UInt128(guid);
        }

        [TestMethod]
        public void ArrayConvertion()
        {
            byte[] array = new byte[16] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
            UInt128 value = array;

            Assert.IsTrue(value.ToArray().SequenceEqual(array));
            Assert.IsTrue(array.SequenceEqual((byte[])value));
            Assert.IsTrue(value.Equals(array));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void NullArrayConvertion()
        {
            _ = new UInt128((byte[])null!);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void EmptyArrayConvertion()
        {
            byte[] array = new byte[0];
            _ = new UInt128(array);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void InvalidArrayConvertion()
        {
            byte[] array = new byte[17];
            _ = new UInt128(array);
        }

        public void InvalidArrayEquals()
        {
            // Invalid value should not fail in Equals
            byte[] array = new byte[17];
            Assert.IsFalse(UInt128.Zero.Equals(array));
        }

        [TestMethod]
        public void BigIntegerConvertion()
        {
            BigInteger bigInteger = BigInteger.Parse("123456789012345678901234567890123456789");
            UInt128 value = bigInteger;

            Assert.AreEqual(value, (UInt128)bigInteger);
            Assert.AreEqual(bigInteger, (BigInteger)value);
            Assert.IsTrue(value.Equals(bigInteger));
        }

        [TestMethod]
        public void DecimalToString()
        {
            UInt128 value = 1234567890;
            Assert.AreEqual(value.ToString(), "1234567890");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void BigIntegerExceedU128()
        {
            BigInteger bigInteger = BigInteger.Parse("9999999999999999999999999999999999999999");
            _ = new UInt128(bigInteger);
        }

        [TestMethod]
        public void InvalidBigIntegerEquals()
        {
            // Invalid value should not fail in Equals
            BigInteger bigInteger = BigInteger.Parse("9999999999999999999999999999999999999999");
            Assert.IsFalse(UInt128.Zero.Equals(bigInteger));
        }

        [TestMethod]
        public void I64Convertion()
        {
            var value = new UInt128(-1L, -2L);
            var (i64a, i64b) = value.ToInt64();

            Assert.AreEqual(i64a, -1L);
            Assert.AreEqual(i64b, -2L);
        }

        [TestMethod]
        public void SingleI64Convertion()
        {
            UInt128 value = -100L;
            var (i64a, i64b) = value.ToInt64();

            Assert.AreEqual(i64a, -100L);
            Assert.AreEqual(i64b, 0L);
        }

        [TestMethod]
        public void U64Convertion()
        {
            var value = new UInt128(1LU, 2LU);
            var (u64a, u64b) = value.ToUInt64();

            Assert.AreEqual(u64a, 1LU);
            Assert.AreEqual(u64b, 2LU);
        }

        [TestMethod]
        public void SingleU64Convertion()
        {
            UInt128 value = 100UL;
            var (u64a, u64b) = value.ToUInt64();

            Assert.AreEqual(u64a, 100UL);
            Assert.AreEqual(u64b, 0UL);
        }

        [TestMethod]
        public void SingleU32Convertion()
        {
            UInt128 value = 100U;
            var (u64a, u64b) = value.ToUInt64();

            Assert.AreEqual(u64a, 100UL);
            Assert.AreEqual(u64b, 0UL);
        }

        [TestMethod]
        public void HashCode()
        {
            UInt128 a = 100;
            UInt128 b = 101;

            Assert.AreNotEqual(a.GetHashCode(), b.GetHashCode());
        }

        [TestMethod]
        public void Equals()
        {
            Assert.IsTrue(UInt128.Zero.Equals(UInt128.Zero));
            Assert.IsTrue(UInt128.Zero.Equals((object?)0));
            Assert.IsTrue(UInt128.Zero.Equals((object?)0U));
            Assert.IsTrue(UInt128.Zero.Equals((object?)0L));
            Assert.IsTrue(UInt128.Zero.Equals((object?)0UL));
            Assert.IsTrue(UInt128.Zero.Equals((object?)new byte[16]));
            Assert.IsTrue(UInt128.Zero.Equals((object?)Guid.Empty));
            Assert.IsFalse(UInt128.Zero.Equals("0"));
            Assert.IsFalse(UInt128.Zero.Equals(new object()));
            Assert.IsFalse(UInt128.Zero.Equals((object?)null!));
        }

        [TestMethod]
        public void LittleEndian()
        {
            var expected = new byte[16] { 86, 52, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

            Assert.IsTrue(expected.SequenceEqual(new UInt128(expected).ToArray()));
            Assert.IsTrue(expected.SequenceEqual(new UInt128(BigInteger.Parse("123456", NumberStyles.HexNumber)).ToArray()));
            Assert.IsTrue(expected.SequenceEqual(new UInt128(new Guid(expected)).ToArray()));
            Assert.IsTrue(expected.SequenceEqual(new UInt128(0x123456).ToArray()));
            Assert.IsTrue(expected.SequenceEqual(new UInt128(0x123456L).ToArray()));
            Assert.IsTrue(expected.SequenceEqual(new UInt128(0x123456UL).ToArray()));
        }

        [TestMethod]
        public void Operators()
        {
            var a = new UInt128(100, 200);
            var b = new UInt128(100, 200);

            Assert.AreEqual(a, b);
            Assert.IsTrue(a.Equals(b));
            Assert.IsTrue(b.Equals(a));
            Assert.IsTrue(a == b);
            Assert.IsTrue(b == a);
            Assert.IsFalse(a != b);
            Assert.IsTrue(a != UInt128.Zero);
        }
    }
}

