using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace TigerBeetle.Tests;

[TestClass]
public class UInt128Tests
{
    /// <summary>
    /// Consistency of U128 across Zig and the language clients.
    /// It must be kept in sync with all platforms.
    /// </summary>
    [TestMethod]
    public void ConsistencyTest()
    {
        // Decimal representation:
        ulong upper = 16637956065805325009;
        ulong lower = 14033694389904319137;
        var u128 = new UInt128(upper, lower);
        Assert.AreEqual("306916117455534265709072286748445483681", u128.ToString());

        // Binary representation:
        byte[] binary = new byte[] {
            0xa1, 0xa2, 0xa3, 0xa4,
            0xb1, 0xb2,
            0xc1, 0xc2,
            0xd1, 0xd2,
            0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6
        };
        Assert.IsTrue(binary.SequenceEqual(u128.ToArray()));

        // GUID representation:
        var guid = Guid.Parse("a1a2a3a4-b1b2-c1c2-d1d2-e1e2e3e4e5e6");
        Assert.AreEqual(guid, u128.ToGuid());
        Assert.AreEqual(u128, guid.ToUInt128());
    }

    [TestMethod]
    public void GuidConversion()
    {
        Guid guid = Guid.Parse("A945C62A-4CC7-425B-B44A-893577632902");
        UInt128 value = guid.ToUInt128();

        Assert.AreEqual(value, guid.ToUInt128());
        Assert.AreEqual(guid, value.ToGuid());
    }

    [TestMethod]
    public void GuidMaxConversion()
    {
        Guid guid = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff");
        UInt128 value = guid.ToUInt128();

        Assert.AreEqual(value, guid.ToUInt128());
        Assert.AreEqual(guid, value.ToGuid());
    }

    [TestMethod]
    public void ArrayConversion()
    {
        byte[] array = new byte[16] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
        UInt128 value = array.ToUInt128();

        Assert.IsTrue(value.ToArray().SequenceEqual(array));
        Assert.IsTrue(array.SequenceEqual(value.ToArray()));
        Assert.IsTrue(value.Equals(array.ToUInt128()));
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentNullException))]
    public void NullArrayConversion()
    {
        byte[] array = null!;
        _ = array.ToUInt128();
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentException))]
    public void EmptyArrayConversion()
    {
        byte[] array = new byte[0];
        _ = array.ToUInt128();
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentException))]
    public void InvalidArrayConversion()
    {
        // Expected ArgumentException.
        byte[] array = new byte[17];
        _ = array.ToUInt128();
    }


    [TestMethod]
    public void BigIntegerConversion()
    {
        var checkConversion = (BigInteger bigInteger) =>
        {
            UInt128 uint128 = bigInteger.ToUInt128();

            Assert.AreEqual(uint128, bigInteger.ToUInt128());
            Assert.AreEqual(bigInteger, uint128.ToBigInteger());
            Assert.IsTrue(uint128.Equals(bigInteger.ToUInt128()));
        };

        checkConversion(BigInteger.Parse("0"));
        checkConversion(BigInteger.Parse("1"));
        checkConversion(BigInteger.Parse("123456789012345678901234567890123456789"));
        checkConversion(new BigInteger(uint.MaxValue));
        checkConversion(new BigInteger(ulong.MaxValue));
    }



    [TestMethod]
    [ExpectedException(typeof(OverflowException))]
    public void BigIntegerNegative()
    {
        // Expected OverflowException.
        _ = BigInteger.MinusOne.ToUInt128();
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentOutOfRangeException))]
    public void BigIntegerExceedU128()
    {
        // Expected ArgumentOutOfRangeException.
        BigInteger bigInteger = BigInteger.Parse("9999999999999999999999999999999999999999");
        _ = bigInteger.ToUInt128();
    }

    [TestMethod]
    public void LittleEndian()
    {
        byte[] bytes_expected = new byte[16] { 0x78, 0x56, 0x34, 0x12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        UInt128 decimal_expected = new UInt128(0, 0x12345678);
        BigInteger bigint_expected = BigInteger.Parse("12345678", NumberStyles.HexNumber);
        Guid guid_expected = Guid.Parse("78563412-0000-0000-0000-000000000000");

        Assert.AreEqual(decimal_expected, bytes_expected.ToUInt128());
        Assert.AreEqual(decimal_expected, bigint_expected.ToUInt128());
        Assert.AreEqual(decimal_expected, guid_expected.ToUInt128());

        Assert.IsTrue(bytes_expected.SequenceEqual(decimal_expected.ToArray()));
        Assert.IsTrue(bytes_expected.SequenceEqual(bytes_expected.ToUInt128().ToArray()));
        Assert.IsTrue(bytes_expected.SequenceEqual(bigint_expected.ToUInt128().ToArray()));
        Assert.IsTrue(bytes_expected.SequenceEqual(guid_expected.ToUInt128().ToArray()));
    }

    [TestMethod]
    public void IDCreation()
    {
        var verifier = () =>
        {
            UInt128 idA = ID.Create();
            for (int i = 0; i < 1_000_000; i++)
            {
                if (i % 1_000 == 0)
                {
                    Thread.Sleep(1);
                }

                UInt128 idB = ID.Create();
                Assert.IsTrue(idB.CompareTo(idA) > 0);
                idA = idB;
            }
        };

        // Verify monotonic IDs locally.
        verifier();

        // Verify monotonic IDs across multiple threads.
        var concurrency = 10;
        var startBarrier = new Barrier(concurrency);
        Parallel.For(0, concurrency, (_, _) =>
        {
            startBarrier.SignalAndWait();
            verifier();
        });
    }
}
