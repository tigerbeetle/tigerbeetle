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
            UInt128 value = guid.ToUInt128();

            Assert.AreEqual(value, guid.ToUInt128());
            Assert.AreEqual(guid, value.ToGuid());
        }

        [TestMethod]
        public void GuidMaxConvertion()
        {
            Guid guid = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff");
			UInt128 value = guid.ToUInt128();

			Assert.AreEqual(value, guid.ToUInt128());
			Assert.AreEqual(guid, value.ToGuid());
		}

        [TestMethod]
        public void ArrayConvertion()
        {
            byte[] array = new byte[16] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
            UInt128 value = array.ToUInt128();

            Assert.IsTrue(value.ToArray().SequenceEqual(array));
            Assert.IsTrue(array.SequenceEqual(value.ToArray()));
            Assert.IsTrue(value.Equals(array.ToUInt128()));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void NullArrayConvertion()
        {
            byte[] array = null!;
            _ = array.ToUInt128();
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void EmptyArrayConvertion()
        {
            byte[] array = new byte[0];
			_ = array.ToUInt128();
		}

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void InvalidArrayConvertion()
        {
            byte[] array = new byte[17];
			_ = array.ToUInt128();
		}


        [TestMethod]
        public void BigIntegerConvertion()
        {
            BigInteger bigInteger = BigInteger.Parse("123456789012345678901234567890123456789");
            UInt128 value = bigInteger.ToUInt128();

            Assert.AreEqual(value, bigInteger.ToUInt128());
            Assert.AreEqual(bigInteger, value.ToBigInteger());
            Assert.IsTrue(value.Equals(bigInteger.ToUInt128()));
        }

        [TestMethod]
        public void DecimalToString()
        {
            UInt128 value = (UInt128)1234567890;
            Assert.AreEqual(value.ToString(), "1234567890");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void BigIntegerExceedU128()
        {
            BigInteger bigInteger = BigInteger.Parse("9999999999999999999999999999999999999999");
            _ = bigInteger.ToUInt128();
        }

        [TestMethod]
        public void LittleEndian()
        {
            var expected = new byte[16] { 86, 52, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

            Assert.IsTrue(expected.SequenceEqual(expected.ToUInt128().ToArray()));
            Assert.IsTrue(expected.SequenceEqual(BigInteger.Parse("123456", NumberStyles.HexNumber).ToUInt128().ToArray()));
            Assert.IsTrue(expected.SequenceEqual(new Guid(expected).ToUInt128().ToArray()));
            Assert.IsTrue(expected.SequenceEqual(new UInt128(0, 0x123456).ToArray()));
        }        

#if !NET7_0_OR_GREATER

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
            // Same type:
            Assert.IsTrue(UInt128.Zero.Equals(UInt128.Zero));
            Assert.IsTrue(((UInt128)1).Equals((UInt128)1));
            Assert.IsTrue(new UInt128(0UL, ulong.MaxValue).Equals(new UInt128(0UL, ulong.MaxValue)));

            // Implicit conversion:
            Assert.IsTrue(UInt128.Zero.Equals(0));
            Assert.IsTrue(((UInt128)1).Equals(1));
            Assert.IsTrue(new UInt128(0UL, ulong.MaxValue).Equals(ulong.MaxValue));

            // Explicit conversion:
            Assert.IsTrue(UInt128.Zero.Equals((int)0));
            Assert.IsTrue(((UInt128)1).Equals((int)1));
            Assert.IsTrue(new UInt128(0UL, long.MaxValue).Equals(long.MaxValue));

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
#endif
    }
}