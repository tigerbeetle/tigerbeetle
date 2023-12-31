using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace TigerBeetle.Tests;

[TestClass]
public class BindingTests
{
    [TestMethod]
    public void Accounts()
    {
        var account = new Account();
        account.Id = 100;
        Assert.AreEqual(account.Id, (UInt128)100);

        account.UserData128 = 101;
        Assert.AreEqual(account.UserData128, (UInt128)101);

        account.UserData64 = 102;
        Assert.AreEqual(account.UserData64, (ulong)102L);

        account.UserData32 = 103;
        Assert.AreEqual(account.UserData32, (uint)103);

        account.Reserved = 0;
        Assert.AreEqual(account.Reserved, (uint)0);

        account.Ledger = 104;
        Assert.AreEqual(account.Ledger, (uint)104);

        account.Code = 105;
        Assert.AreEqual(account.Code, (ushort)105);

        var flags = AccountFlags.Linked | AccountFlags.DebitsMustNotExceedCredits | AccountFlags.CreditsMustNotExceedDebits;
        account.Flags = flags;
        Assert.AreEqual(account.Flags, flags);

        account.DebitsPending = 1001;
        Assert.AreEqual(account.DebitsPending, (UInt128)1001);

        account.CreditsPending = 1002;
        Assert.AreEqual(account.CreditsPending, (UInt128)1002);

        account.DebitsPosted = 1003;
        Assert.AreEqual(account.DebitsPosted, (UInt128)1003);

        account.CreditsPosted = 1004;
        Assert.AreEqual(account.CreditsPosted, (UInt128)1004);

        account.Timestamp = 99_999;
        Assert.AreEqual(account.Timestamp, (ulong)99_999);
    }

    [TestMethod]
    public void AccountsDefault()
    {
        var account = new Account(); ;
        Assert.AreEqual(account.Id, UInt128.Zero);
        Assert.AreEqual(account.UserData128, UInt128.Zero);
        Assert.AreEqual(account.UserData64, (ulong)0);
        Assert.AreEqual(account.UserData32, (uint)0);
        Assert.AreEqual(account.Reserved, (uint)0);
        Assert.AreEqual(account.Ledger, (uint)0);
        Assert.AreEqual(account.Code, (ushort)0);
        Assert.AreEqual(account.Flags, AccountFlags.None);
        Assert.AreEqual(account.DebitsPending, (UInt128)0);
        Assert.AreEqual(account.CreditsPending, (UInt128)0);
        Assert.AreEqual(account.DebitsPosted, (UInt128)0);
        Assert.AreEqual(account.CreditsPosted, (UInt128)0);
        Assert.AreEqual(account.Timestamp, (UInt128)0);
    }

    [TestMethod]
    public void AccountsSerialize()
    {
        var expected = new byte[Account.SIZE];
        using (var writer = new BinaryWriter(new MemoryStream(expected)))
        {
            writer.Write(10L); // Id (lsb)
            writer.Write(11L); // Id (msb)
            writer.Write(100L); // DebitsPending (lsb)
            writer.Write(110L); // DebitsPending (msb)
            writer.Write(200L); // DebitsPosted (lsb)
            writer.Write(210L); // DebitsPosted (msb)
            writer.Write(300L); // CreditPending (lsb)
            writer.Write(310L); // CreditPending (msb)
            writer.Write(400L); // CreditsPosted (lsb)
            writer.Write(410L); // CreditsPosted (msb)
            writer.Write(1000L); // UserData128 (lsb)
            writer.Write(1100L); // UserData128 (msb)
            writer.Write(2000L); // UserData64
            writer.Write(3000); // UserData32
            writer.Write(0); // Reserved
            writer.Write(720); // Ledger
            writer.Write((short)1); // Code
            writer.Write((short)1); // Flags
            writer.Write(999L); // Timestamp
        }

        var account = new Account
        {
            Id = new UInt128(11L, 10L),
            DebitsPending = new UInt128(110L, 100L),
            DebitsPosted = new UInt128(210L, 200L),
            CreditsPending = new UInt128(310L, 300L),
            CreditsPosted = new UInt128(410L, 400L),
            UserData128 = new UInt128(1100L, 1000L),
            UserData64 = 2000L,
            UserData32 = 3000,
            Ledger = 720,
            Code = 1,
            Flags = AccountFlags.Linked,
            Timestamp = 999,
        };

        var serialized = MemoryMarshal.AsBytes<Account>(new Account[] { account }).ToArray();
        Assert.IsTrue(expected.SequenceEqual(serialized));
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
        Assert.AreEqual(transfer.Id, (UInt128)100);

        transfer.DebitAccountId = 101;
        Assert.AreEqual(transfer.DebitAccountId, (UInt128)101);

        transfer.CreditAccountId = 102;
        Assert.AreEqual(transfer.CreditAccountId, (UInt128)102);

        transfer.Amount = 1001;
        Assert.AreEqual(transfer.Amount, (UInt128)1001);

        transfer.PendingId = 103;
        Assert.AreEqual(transfer.PendingId, (UInt128)103);

        transfer.UserData128 = 104;
        Assert.AreEqual(transfer.UserData128, (UInt128)104);

        transfer.UserData64 = 105;
        Assert.AreEqual(transfer.UserData64, (ulong)105);

        transfer.UserData32 = 106;
        Assert.AreEqual(transfer.UserData32, (uint)106);

        transfer.Timeout = 107;
        Assert.AreEqual(transfer.Timeout, (uint)107);

        transfer.Ledger = 108;
        Assert.AreEqual(transfer.Ledger, (uint)108);

        transfer.Code = 109;
        Assert.AreEqual(transfer.Code, (ushort)109);

        var flags = TransferFlags.Linked | TransferFlags.PostPendingTransfer | TransferFlags.VoidPendingTransfer;
        transfer.Flags = flags;
        Assert.AreEqual(transfer.Flags, flags);

        transfer.Timestamp = 99_999;
        Assert.AreEqual(transfer.Timestamp, (ulong)99_999);
    }

    [TestMethod]
    public void TransferDefault()
    {
        var transfer = new Transfer();
        Assert.AreEqual(transfer.Id, (UInt128)0);
        Assert.AreEqual(transfer.DebitAccountId, (UInt128)0);
        Assert.AreEqual(transfer.CreditAccountId, (UInt128)0);
        Assert.AreEqual(transfer.Amount, (UInt128)0);
        Assert.AreEqual(transfer.PendingId, (UInt128)0);
        Assert.AreEqual(transfer.UserData128, (UInt128)0);
        Assert.AreEqual(transfer.UserData64, (ulong)0);
        Assert.AreEqual(transfer.UserData32, (uint)0);
        Assert.AreEqual(transfer.Timeout, (uint)0);
        Assert.AreEqual(transfer.Ledger, (uint)0);
        Assert.AreEqual(transfer.Code, (ushort)0);
        Assert.AreEqual(transfer.Flags, TransferFlags.None);
        Assert.AreEqual(transfer.Timestamp, (ulong)0);
    }

    [TestMethod]
    public void TransfersSerialize()
    {
        var expected = new byte[Transfer.SIZE];
        using (var writer = new BinaryWriter(new MemoryStream(expected)))
        {
            writer.Write(10L); // Id (lsb)
            writer.Write(11L); // Id (msb)
            writer.Write(100L); // DebitAccountId (lsb)
            writer.Write(110L); // DebitAccountId (msb)
            writer.Write(200L); // CreditAccountId (lsb)
            writer.Write(210L); // CreditAccountId (msb)
            writer.Write(300L); // Amount (lsb)
            writer.Write(310L); // Amount (msb)
            writer.Write(400L); // PendingId (lsb)
            writer.Write(410L); // PendingId (msb)
            writer.Write(1000L); // UserData128 (lsb)
            writer.Write(1100L); // UserData128 (msb)
            writer.Write(2000L); // UserData64
            writer.Write(3000); // UserData32
            writer.Write(999); // Timeout
            writer.Write(720); // Ledger
            writer.Write((short)1); // Code
            writer.Write((short)1); // Flags
            writer.Write(99_999L); // Timestamp
        }

        var transfer = new Transfer
        {
            Id = new UInt128(11L, 10L),
            DebitAccountId = new UInt128(110L, 100L),
            CreditAccountId = new UInt128(210L, 200L),
            Amount = new UInt128(310L, 300L),
            PendingId = new UInt128(410L, 400L),
            UserData128 = new UInt128(1100L, 1000L),
            UserData64 = 2000L,
            UserData32 = 3000,
            Timeout = 999,
            Ledger = 720,
            Code = 1,
            Flags = TransferFlags.Linked,
            Timestamp = 99_999,
        };

        var serialized = MemoryMarshal.AsBytes<Transfer>(new Transfer[] { transfer }).ToArray();
        Assert.IsTrue(expected.SequenceEqual(serialized));
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
