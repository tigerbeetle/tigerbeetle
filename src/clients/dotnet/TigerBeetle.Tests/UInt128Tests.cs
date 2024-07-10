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
        var expected = new byte[16] { 86, 52, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

        Assert.IsTrue(expected.SequenceEqual(expected.ToUInt128().ToArray()));
        Assert.IsTrue(expected.SequenceEqual(BigInteger.Parse("123456", NumberStyles.HexNumber).ToUInt128().ToArray()));
        Assert.IsTrue(expected.SequenceEqual(new Guid(expected).ToUInt128().ToArray()));
        Assert.IsTrue(expected.SequenceEqual(new UInt128(0, 0x123456).ToArray()));
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
