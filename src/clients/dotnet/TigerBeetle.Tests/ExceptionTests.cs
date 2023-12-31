using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace TigerBeetle.Tests;

[TestClass]
public class ExceptionTests
{
    [TestMethod]
    public void AssertTrue()
    {
        // Should not throw an exception
        // when the condition is true.
        AssertionException.AssertTrue(true);
    }

    [TestMethod]
    [ExpectedException(typeof(AssertionException))]
    public void AssertFalse()
    {
        // Expected AssertionException.
        AssertionException.AssertTrue(false);
    }

    [TestMethod]
    public void AssertFalseWithMessage()
    {
        try
        {
            AssertionException.AssertTrue(false, "hello {0}", "world");

            // Should not be reachable:
            Assert.IsTrue(false);
        }
        catch (AssertionException exception)
        {
            Assert.AreEqual("hello world", exception.Message);
        }
    }

    [TestMethod]
    public void AssertTrueWithMessage()
    {
        AssertionException.AssertTrue(true, "unreachable");
    }

    [TestMethod]
    public void InitializationException()
    {
        foreach (InitializationStatus status in (InitializationStatus[])Enum.GetValues(typeof(InitializationStatus)))
        {
            var exception = new InitializationException(status);
            var unknownMessage = "Unknown error status " + status;
            if (status == InitializationStatus.Success)
            {
                Assert.AreEqual(unknownMessage, exception.Message);
            }
            else
            {
                Assert.AreNotEqual(unknownMessage, exception.Message);
            }
        }
    }

    [TestMethod]
    public void RequestException()
    {
        foreach (PacketStatus status in (PacketStatus[])Enum.GetValues(typeof(PacketStatus)))
        {
            var exception = new RequestException(status);
            var unknownMessage = "Unknown error status " + status;
            if (status == PacketStatus.Ok)
            {
                Assert.AreEqual(unknownMessage, exception.Message);
            }
            else
            {
                Assert.AreNotEqual(unknownMessage, exception.Message);
            }
        }
    }
}
