using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace TigerBeetle.Tests
{
    [TestClass]
    public class UInt128Tests
    {
        [TestMethod]
        public void Convertion()
        {
            Guid guid = Guid.NewGuid();
            UInt128 value = guid;

            Assert.AreEqual(value, (UInt128)guid);
            Assert.AreEqual(guid, (Guid)value);
        }
    }
}
