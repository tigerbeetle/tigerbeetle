using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

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
        public void AccountsDefault()
        {
            var account = new Account(); ;
            Assert.AreEqual(account.Id, UInt128.Zero);
            Assert.AreEqual(account.UserData, UInt128.Zero);
            Assert.IsTrue(new byte[48].SequenceEqual(account.Reserved));
            Assert.AreEqual(account.Ledger, (uint)0);
            Assert.AreEqual(account.Code, (ushort)0);
            Assert.AreEqual(account.Flags, AccountFlags.None);
            Assert.AreEqual(account.DebitsPending, (ulong)0);
            Assert.AreEqual(account.CreditsPending, (ulong)0);
            Assert.AreEqual(account.DebitsPosted, (ulong)0);
            Assert.AreEqual(account.CreditsPosted, (ulong)0);
            Assert.AreEqual(account.Timestamp, (ulong)0);
        }

        [TestMethod]
        public void AccountsSerialize()
        {
            var x = BitConverter.GetBytes(10L);
            var y = BitConverter.GetBytes(100L);

            var expected = new byte[Account.SIZE];
            using (var writer = new BinaryWriter(new MemoryStream(expected)))
            {
                writer.Write(10L); // Id (lsb)
                writer.Write(100L); // Id (msb)
                writer.Write(1000L); // UserData (lsb)
                writer.Write(1100L); // UserData (msb)
                writer.Write(new byte[48]); // Reserved
                writer.Write(720); // Ledger
                writer.Write((short)1); // Code
                writer.Write((short)1); // Flags
                writer.Write(100L); // DebitsPending
                writer.Write(200L); // DebitsPosted
                writer.Write(300L); // CreditPending
                writer.Write(400L); // CreditsPosted
                writer.Write(999L); // Timestamp
            }

            var account = new Account
            {
                Id = new UInt128(10L, 100L),
                UserData = new UInt128(1000L, 1100L),
                Ledger = 720,
                Code = 1,
                Flags = AccountFlags.Linked,
                DebitsPending = 100,
                DebitsPosted = 200,
                CreditsPending = 300,
                CreditsPosted = 400,
                Timestamp = 999,
            };

            var serialized = MemoryMarshal.AsBytes<Account>(new Account[] { account }).ToArray();
            Assert.IsTrue(expected.SequenceEqual(serialized));
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
        public void TransferDefault()
        {
            var transfer = new Transfer();
            Assert.AreEqual(transfer.Id, UInt128.Zero);
            Assert.AreEqual(transfer.DebitAccountId, UInt128.Zero);
            Assert.AreEqual(transfer.CreditAccountId, UInt128.Zero);
            Assert.AreEqual(transfer.UserData, UInt128.Zero);
            Assert.AreEqual(transfer.Reserved, UInt128.Zero);
            Assert.AreEqual(transfer.PendingId, UInt128.Zero);
            Assert.AreEqual(transfer.Timeout, (ulong)0);
            Assert.AreEqual(transfer.Ledger, (uint)0);
            Assert.AreEqual(transfer.Code, (ushort)0);
            Assert.AreEqual(transfer.Flags, TransferFlags.None);
            Assert.AreEqual(transfer.Amount, (ulong)0);
            Assert.AreEqual(transfer.Timestamp, (ulong)0);
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
