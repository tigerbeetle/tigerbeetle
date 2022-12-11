using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using TigerBeetle;

namespace TigerBeetle.Tests
{
    [TestClass]
    public class BindingTests
    {
        [TestMethod]
        public void Accounts()
        {
            var account = new Account();
            account.Id = 100;
            Assert.AreEqual(account.Id, new UInt128(100));

            account.UserData = 101;
            Assert.AreEqual(account.UserData, new UInt128(101));

            var reserved = Enumerable.Range(0, 48).Select(x => (byte)x).ToArray();
            account.Reserved = reserved;
            Assert.IsTrue(reserved.SequenceEqual(account.Reserved));

            account.Ledger = 102;
            Assert.AreEqual(account.Ledger, (uint)102);

            account.Code = 103;
            Assert.AreEqual(account.Code, (ushort)103);

            var flags = AccountFlags.Linked | AccountFlags.DebitsMustNotExceedCredits | AccountFlags.CreditsMustNotExceedDebits;
            account.Flags = flags;
            Assert.AreEqual(account.Flags, flags);

            account.DebitsPending = 1001;
            Assert.AreEqual(account.DebitsPending, (ulong)1001);

            account.CreditsPending = 1002;
            Assert.AreEqual(account.CreditsPending, (ulong)1002);

            account.DebitsPosted = 1003;
            Assert.AreEqual(account.DebitsPosted, (ulong)1003);

            account.CreditsPosted = 1004;
            Assert.AreEqual(account.CreditsPosted, (ulong)1004);

            account.Timestamp = 99_999;
            Assert.AreEqual(account.Timestamp, (ulong)99_999);
        }

        [TestMethod]
        public void InvalidAccountReservedValues()
        {
            var account = new Account();
            Assert.ThrowsException<ArgumentNullException>(() => account.Reserved = null!);
            Assert.ThrowsException<ArgumentException>(() => account.Reserved = new byte[0]);
            Assert.ThrowsException<ArgumentException>(() => account.Reserved = new byte[47]);
            Assert.ThrowsException<ArgumentException>(() => account.Reserved = new byte[49]);
        }        

        [TestMethod]
        public void CreateAccountsResults()
        {
            var result = new CreateAccountsResult();

            result.Index = 1;
            Assert.AreEqual(result.Index, (uint)1);

            result.Result = CreateAccountResult.Exists;
            Assert.AreEqual(result.Result, CreateAccountResult.Exists);
        }

        [TestMethod]
        public void Transfers()
        {
            var transfer = new Transfer();

            transfer.Id = 100;
            Assert.AreEqual(transfer.Id, new UInt128(100));

            transfer.DebitAccountId = 101;
            Assert.AreEqual(transfer.DebitAccountId, new UInt128(101));

            transfer.CreditAccountId = 102;
            Assert.AreEqual(transfer.CreditAccountId, new UInt128(102));

            transfer.UserData = 103;
            Assert.AreEqual(transfer.UserData, new UInt128(103));

            transfer.Reserved = 104;
            Assert.AreEqual(transfer.Reserved, new UInt128(104));

            transfer.PendingId = 105;
            Assert.AreEqual(transfer.PendingId, new UInt128(105));

            transfer.Timeout = 106;
            Assert.AreEqual(transfer.Timeout, (ulong)106);

            transfer.Ledger = 102;
            Assert.AreEqual(transfer.Ledger, (uint)102);

            transfer.Code = 103;
            Assert.AreEqual(transfer.Code, (ushort)103);

            var flags = TransferFlags.Linked | TransferFlags.PostPendingTransfer | TransferFlags.VoidPendingTransfer;
            transfer.Flags = flags;
            Assert.AreEqual(transfer.Flags, flags);

            transfer.Amount = 1001;
            Assert.AreEqual(transfer.Amount, (ulong)1001);

            transfer.Timestamp = 99_999;
            Assert.AreEqual(transfer.Timestamp, (ulong)99_999);
        }

        [TestMethod]
        public void CreateTransfersResults()
        {
            var result = new CreateTransfersResult();

            result.Index = 1;
            Assert.AreEqual(result.Index, (uint)1);

            result.Result = CreateTransferResult.Exists;
            Assert.AreEqual(result.Result, CreateTransferResult.Exists);
        }
    }
}
