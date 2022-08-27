using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace TigerBeetle.Tests
{
    [TestClass]
    public class UInt128Tests
    {
        [TestMethod]
        public void GuidConvertion()
        {
            Guid guid = Guid.NewGuid();
            UInt128 value = guid;

            Assert.AreEqual(value, (UInt128)guid);
            Assert.AreEqual(guid, (Guid)value);
        }

        [TestMethod]
        public void I64Constructor()
        {
            UInt128 value = new UInt128(-1L, -2L);
            var (i64a, i64b) = value.ToInt64();

            Assert.AreEqual(i64a, -1L);
            Assert.AreEqual(i64b, -2L);
        } 

        [TestMethod]
        public void U64Constructors()
        {
            UInt128 value = new UInt128(1LU, 2LU);
            var (u64a, u64b) = value.ToUInt64();

            Assert.AreEqual(u64a, 1LU);
            Assert.AreEqual(u64b, 2LU);
        }                
    }
}
