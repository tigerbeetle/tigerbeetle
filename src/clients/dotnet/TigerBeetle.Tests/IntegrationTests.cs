using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;

namespace TigerBeetle.Tests;

[TestClass]
public class IntegrationTests
{
    private static Client GetClient(string address, int concurrencyMax = 32) => new(0, new string[] { address }, concurrencyMax);

    private static readonly Account[] accounts = new[]
    {
            new Account
            {
                Id = 100,
                UserData128 = 1000,
                UserData64 = 1001,
                UserData32 = 1002,
                Flags = AccountFlags.None,
                Ledger = 1,
                Code = 1,
            },
            new Account
            {
                Id = 101,
                UserData128 = 1000,
                UserData64 = 1001,
                UserData32 = 1002,
                Flags = AccountFlags.None,
                Ledger = 1,
                Code = 2,
            }
        };

    [TestMethod]
    [ExpectedException(typeof(ArgumentNullException))]
    public void ConstructorWithNullReplicaAddresses()
    {
        string[]? addresses = null;
        _ = new Client(0, addresses!, 1);
    }

    [TestMethod]
    public void ConstructorWithNullReplicaAddressElement()
    {
        try
        {
            var addresses = new string?[] { "3000", null };
            _ = new Client(0, addresses!, 1);
            Assert.IsTrue(false);
        }
        catch (InitializationException exception)
        {
            Assert.AreEqual(InitializationStatus.AddressInvalid, exception.Status);
        }
    }

    [TestMethod]
    public void ConstructorWithEmptyReplicaAddresses()
    {
        try
        {
            _ = new Client(0, Array.Empty<string>(), 1);
            Assert.IsTrue(false);
        }
        catch (InitializationException exception)
        {
            Assert.AreEqual(InitializationStatus.AddressInvalid, exception.Status);
        }
    }

    [TestMethod]
    public void ConstructorWithEmptyReplicaAddressElement()
    {
        try
        {
            _ = new Client(0, new string[] { "" }, 1);
            Assert.IsTrue(false);
        }
        catch (InitializationException exception)
        {
            Assert.AreEqual(InitializationStatus.AddressInvalid, exception.Status);
        }
    }

    [TestMethod]
    public void ConstructorWithInvalidReplicaAddresses()
    {
        try
        {
            var addresses = Enumerable.Range(3000, 3100).Select(x => x.ToString()).ToArray();
            _ = new Client(0, addresses, 1);
            Assert.IsTrue(false);
        }
        catch (InitializationException exception)
        {
            Assert.AreEqual(InitializationStatus.AddressLimitExceeded, exception.Status);
        }
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentOutOfRangeException))]
    public void ConstructorWithZeroConcurrencyMax()
    {
        _ = new Client(0, new string[] { "3000" }, 0);
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentOutOfRangeException))]
    public void ConstructorWithNegativeConcurrencyMax()
    {
        _ = new Client(0, new string[] { "3000" }, -1);
    }

    [TestMethod]
    public void ConstructorWithInvalidConcurrencyMax()
    {
        try
        {
            _ = new Client(0, new string[] { "3000" }, 99_999);
            Assert.IsTrue(false);
        }
        catch (InitializationException exception)
        {
            Assert.AreEqual(InitializationStatus.ConcurrencyMaxInvalid, exception.Status);
        }
    }

    [TestMethod]
    public void ConstructorAndFinalizer()
    {
        // No using here, we want to test the finalizer
        var client = new Client(1, new string[] { "3000" }, 32);
        Assert.IsTrue(client.ClusterID == 1);
    }

    [TestMethod]
    [DoNotParallelize]
    public void CreateAccount()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var okResult = client.CreateAccount(accounts[0]);
        Assert.IsTrue(okResult == CreateAccountResult.Ok);

        var lookupAccount = client.LookupAccount(accounts[0].Id);
        Assert.IsNotNull(lookupAccount);
        AssertAccount(accounts[0], lookupAccount!.Value);

        var existsResult = client.CreateAccount(accounts[0]);
        Assert.IsTrue(existsResult == CreateAccountResult.Exists);
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task CreateAccountAsync()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var okResult = await client.CreateAccountAsync(accounts[0]);
        Assert.IsTrue(okResult == CreateAccountResult.Ok);

        var lookupAccount = await client.LookupAccountAsync(accounts[0].Id);
        Assert.IsNotNull(lookupAccount);
        AssertAccount(accounts[0], lookupAccount!.Value);

        var existsResult = await client.CreateAccountAsync(accounts[0]);
        Assert.IsTrue(existsResult == CreateAccountResult.Exists);
    }

    [TestMethod]
    [DoNotParallelize]
    public void CreateAccounts()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var results = client.CreateAccounts(accounts);
        Assert.IsTrue(results.Length == 0);

        var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task CreateAccountsAsync()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var results = await client.CreateAccountsAsync(accounts);
        Assert.IsTrue(results.Length == 0);

        var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);
    }

    [TestMethod]
    [DoNotParallelize]
    public void CreateTransfers()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var accountResults = client.CreateAccounts(accounts);
        Assert.IsTrue(accountResults.Length == 0);

        var transfer = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Ledger = 1,
            Code = 1,
        };

        var transferResults = client.CreateTransfers(new Transfer[] { transfer });
        Assert.IsTrue(transferResults.Length == 0);

        var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        var lookupTransfers = client.LookupTransfers(new UInt128[] { transfer.Id });
        Assert.IsTrue(lookupTransfers.Length == 1);
        AssertTransfer(transfer, lookupTransfers[0]);

        Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer.Amount);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, (UInt128)0);

        Assert.AreEqual(lookupAccounts[1].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer.Amount);
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task CreateTransfersAsync()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var accountResults = await client.CreateAccountsAsync(accounts);
        Assert.IsTrue(accountResults.Length == 0);

        var transfer = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Ledger = 1,
            Code = 1,
        };

        var transferResults = await client.CreateTransfersAsync(new Transfer[] { transfer });
        Assert.IsTrue(transferResults.Length == 0);

        var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        var lookupTransfers = await client.LookupTransfersAsync(new UInt128[] { transfer.Id });
        Assert.IsTrue(lookupTransfers.Length == 1);
        AssertTransfer(transfer, lookupTransfers[0]);

        Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer.Amount);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, (UInt128)0);

        Assert.AreEqual(lookupAccounts[1].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer.Amount);
    }

    [TestMethod]
    [DoNotParallelize]
    public void CreateTransfer()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var results = client.CreateAccounts(accounts);
        Assert.IsTrue(results.Length == 0);

        var transfer = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Ledger = 1,
            Code = 1,
        };

        var successResult = client.CreateTransfer(transfer);
        Assert.IsTrue(successResult == CreateTransferResult.Ok);

        var lookupTransfer = client.LookupTransfer(transfer.Id);
        Assert.IsTrue(lookupTransfer != null);
        AssertTransfer(transfer, lookupTransfer!.Value);

        var existsResult = client.CreateTransfer(transfer);
        Assert.IsTrue(existsResult == CreateTransferResult.Exists);
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task CreateTransferAsync()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var results = await client.CreateAccountsAsync(accounts);
        Assert.IsTrue(results.Length == 0);

        var transfer = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Ledger = 1,
            Code = 1,
        };

        var successResult = await client.CreateTransferAsync(transfer);
        Assert.IsTrue(successResult == CreateTransferResult.Ok);

        var lookupTransfer = await client.LookupTransferAsync(transfer.Id);
        Assert.IsTrue(lookupTransfer != null);
        AssertTransfer(transfer, lookupTransfer!.Value);

        var existsResult = await client.CreateTransferAsync(transfer);
        Assert.IsTrue(existsResult == CreateTransferResult.Exists);
    }

    [TestMethod]
    [DoNotParallelize]
    public void CreatePendingTransfers()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var results = client.CreateAccounts(accounts);
        Assert.IsTrue(results.Length == 0);

        var transfer = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Timeout = uint.MaxValue,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Pending,
        };

        var result = client.CreateTransfer(transfer);
        Assert.IsTrue(result == CreateTransferResult.Ok);

        var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        Assert.AreEqual(lookupAccounts[0].CreditsPending, transfer.Amount);
        Assert.AreEqual(lookupAccounts[0].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, (UInt128)0);

        Assert.AreEqual(lookupAccounts[1].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPending, transfer.Amount);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, (UInt128)0);

        var postTransfer = new Transfer
        {
            Id = 2,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            PendingId = transfer.Id,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.PostPendingTransfer,
        };

        var postResult = client.CreateTransfer(postTransfer);
        Assert.IsTrue(postResult == CreateTransferResult.Ok);

        lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer.Amount);
        Assert.AreEqual(lookupAccounts[0].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPending, (UInt128)0);

        Assert.AreEqual(lookupAccounts[1].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer.Amount);
        Assert.AreEqual(lookupAccounts[1].DebitsPending, (UInt128)0);
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task CreatePendingTransfersAsync()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var results = await client.CreateAccountsAsync(accounts);
        Assert.IsTrue(results.Length == 0);

        var transfer = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Timeout = uint.MaxValue,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Pending,
        };

        var result = await client.CreateTransferAsync(transfer);
        Assert.IsTrue(result == CreateTransferResult.Ok);

        var lookupAccounts = await client.LookupAccountsAsync(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        Assert.AreEqual(lookupAccounts[0].CreditsPending, transfer.Amount);
        Assert.AreEqual(lookupAccounts[0].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, (UInt128)0);

        Assert.AreEqual(lookupAccounts[1].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPending, transfer.Amount);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, (UInt128)0);

        var postTransfer = new Transfer
        {
            Id = 2,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            PendingId = transfer.Id,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.PostPendingTransfer,
        };

        var postResult = await client.CreateTransferAsync(postTransfer);
        Assert.IsTrue(postResult == CreateTransferResult.Ok);

        lookupAccounts = await client.LookupAccountsAsync(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer.Amount);
        Assert.AreEqual(lookupAccounts[0].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPending, (UInt128)0);

        Assert.AreEqual(lookupAccounts[1].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer.Amount);
        Assert.AreEqual(lookupAccounts[1].DebitsPending, (UInt128)0);
    }

    [TestMethod]
    [DoNotParallelize]
    public void CreatePendingTransfersAndVoid()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var results = client.CreateAccounts(accounts);
        Assert.IsTrue(results.Length == 0);

        var transfer = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Timeout = uint.MaxValue,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Pending,
        };

        var result = client.CreateTransfer(transfer);
        Assert.IsTrue(result == CreateTransferResult.Ok);

        var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        Assert.AreEqual(lookupAccounts[0].CreditsPending, transfer.Amount);
        Assert.AreEqual(lookupAccounts[0].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, (UInt128)0);

        Assert.AreEqual(lookupAccounts[1].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPending, transfer.Amount);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, (UInt128)0);

        var postTransfer = new Transfer
        {
            Id = 2,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.VoidPendingTransfer,
            PendingId = transfer.Id,
        };

        var postResult = client.CreateTransfer(postTransfer);
        Assert.IsTrue(postResult == CreateTransferResult.Ok);

        lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        Assert.AreEqual(lookupAccounts[0].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPending, (UInt128)0);

        Assert.AreEqual(lookupAccounts[1].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPending, (UInt128)0);
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task CreatePendingTransfersAndVoidAsync()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var results = await client.CreateAccountsAsync(accounts);
        Assert.IsTrue(results.Length == 0);

        var transfer = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Timeout = uint.MaxValue,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Pending,
        };

        var result = await client.CreateTransferAsync(transfer);
        Assert.IsTrue(result == CreateTransferResult.Ok);

        var lookupAccounts = await client.LookupAccountsAsync(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        Assert.AreEqual(lookupAccounts[0].CreditsPending, transfer.Amount);
        Assert.AreEqual(lookupAccounts[0].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, (UInt128)0);

        Assert.AreEqual(lookupAccounts[1].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPending, transfer.Amount);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, (UInt128)0);

        var postTransfer = new Transfer
        {
            Id = 2,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            PendingId = transfer.Id,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.VoidPendingTransfer,
        };

        var postResult = await client.CreateTransferAsync(postTransfer);
        Assert.IsTrue(postResult == CreateTransferResult.Ok);

        lookupAccounts = await client.LookupAccountsAsync(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        Assert.AreEqual(lookupAccounts[0].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPending, (UInt128)0);

        Assert.AreEqual(lookupAccounts[1].CreditsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPending, (UInt128)0);
    }

    [TestMethod]
    [DoNotParallelize]
    public void CreateLinkedTransfers()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var results = client.CreateAccounts(accounts);
        Assert.IsTrue(results.Length == 0);

        var transfer1 = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Linked,
        };

        var transfer2 = new Transfer
        {
            Id = 2,
            CreditAccountId = accounts[1].Id,
            DebitAccountId = accounts[0].Id,
            Amount = 49,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.None,
        };

        var transferResults = client.CreateTransfers(new[] { transfer1, transfer2 });
        Assert.IsTrue(transferResults.All(x => x.Result == CreateTransferResult.Ok));

        var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        var lookupTransfers = client.LookupTransfers(new UInt128[] { transfer1.Id, transfer2.Id });
        Assert.IsTrue(lookupTransfers.Length == 2);
        AssertTransfer(transfer1, lookupTransfers[0]);
        AssertTransfer(transfer2, lookupTransfers[1]);

        Assert.AreEqual(lookupAccounts[0].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer1.Amount);
        Assert.AreEqual(lookupAccounts[0].DebitsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, transfer2.Amount);

        Assert.AreEqual(lookupAccounts[1].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].CreditsPosted, transfer2.Amount);
        Assert.AreEqual(lookupAccounts[1].DebitsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer1.Amount);
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task CreateLinkedTransfersAsync()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var results = await client.CreateAccountsAsync(accounts);
        Assert.IsTrue(results.Length == 0);

        var transfer1 = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Linked,
        };

        var transfer2 = new Transfer
        {
            Id = 2,
            CreditAccountId = accounts[1].Id,
            DebitAccountId = accounts[0].Id,
            Amount = 49,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.None,
        };

        var transferResults = await client.CreateTransfersAsync(new[] { transfer1, transfer2 });
        Assert.IsTrue(transferResults.All(x => x.Result == CreateTransferResult.Ok));

        var lookupAccounts = await client.LookupAccountsAsync(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        var lookupTransfers = await client.LookupTransfersAsync(new UInt128[] { transfer1.Id, transfer2.Id });
        Assert.IsTrue(lookupTransfers.Length == 2);
        AssertTransfer(transfer1, lookupTransfers[0]);
        AssertTransfer(transfer2, lookupTransfers[1]);

        Assert.AreEqual(lookupAccounts[0].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer1.Amount);
        Assert.AreEqual(lookupAccounts[0].DebitsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, transfer2.Amount);

        Assert.AreEqual(lookupAccounts[1].CreditsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].CreditsPosted, transfer2.Amount);
        Assert.AreEqual(lookupAccounts[1].DebitsPending, (UInt128)0);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer1.Amount);
    }


    [TestMethod]
    [DoNotParallelize]
    public void CreateAccountTooMuchData()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        const int TOO_MUCH_DATA = 10000;
        var accounts = new Account[TOO_MUCH_DATA];
        for (int i = 0; i < TOO_MUCH_DATA; i++)
        {
            accounts[i] = new Account
            {
                Id = Guid.NewGuid().ToUInt128(),
                Code = 1,
                Ledger = 1
            };
        }

        try
        {
            _ = client.CreateAccounts(accounts);
            Assert.IsTrue(false);
        }
        catch (RequestException requestException)
        {
            Assert.AreEqual(PacketStatus.TooMuchData, requestException.Status);
        }
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task CreateAccountTooMuchDataAsync()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        const int TOO_MUCH_DATA = 10000;
        var accounts = new Account[TOO_MUCH_DATA];
        for (int i = 0; i < TOO_MUCH_DATA; i++)
        {
            accounts[i] = new Account
            {
                Id = Guid.NewGuid().ToUInt128(),
                Code = 1,
                Ledger = 1
            };
        }

        try
        {
            _ = await client.CreateAccountsAsync(accounts);
            Assert.IsTrue(false);
        }
        catch (RequestException requestException)
        {
            Assert.AreEqual(PacketStatus.TooMuchData, requestException.Status);
        }
    }

    [TestMethod]
    [DoNotParallelize]
    public void CreateTransferTooMuchData()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        const int TOO_MUCH_DATA = 10000;
        var transfers = new Transfer[TOO_MUCH_DATA];
        for (int i = 0; i < TOO_MUCH_DATA; i++)
        {
            transfers[i] = new Transfer
            {
                Id = Guid.NewGuid().ToUInt128(),
                Code = 1,
                Ledger = 1
            };
        }

        try
        {
            _ = client.CreateTransfers(transfers);
            Assert.IsTrue(false);
        }
        catch (RequestException requestException)
        {
            Assert.AreEqual(PacketStatus.TooMuchData, requestException.Status);
        }
    }

    [TestMethod]
    [DoNotParallelize]
    public async Task CreateTransferTooMuchDataAsync()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        const int TOO_MUCH_DATA = 10000;
        var transfers = new Transfer[TOO_MUCH_DATA];
        for (int i = 0; i < TOO_MUCH_DATA; i++)
        {
            transfers[i] = new Transfer
            {
                Id = Guid.NewGuid().ToUInt128(),
                DebitAccountId = accounts[0].Id,
                CreditAccountId = accounts[1].Id,
                Code = 1,
                Ledger = 1,
                Amount = 100,
            };
        }

        try
        {
            _ = await client.CreateTransfersAsync(transfers);
            Assert.IsTrue(false);
        }
        catch (RequestException requestException)
        {
            Assert.AreEqual(PacketStatus.TooMuchData, requestException.Status);
        }
    }


    [TestMethod]
    [DoNotParallelize]
    public void TestGetAccountTransfers()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        // Creating the accounts.
        var createAccountErrors = client.CreateAccounts(accounts);
        Assert.IsTrue(createAccountErrors.Length == 0);

        // Creating a transfer.
        var transfers = new Transfer[10];
        for (int i = 0; i < 10; i++)
        {
            transfers[i] = new Transfer
            {
                Id = (UInt128)(i + 1),

                // Swap the debit and credit accounts:
                CreditAccountId = accounts[i % 2 == 0 ? 0 : 1].Id,
                DebitAccountId = accounts[i % 2 == 0 ? 1 : 0].Id,

                Ledger = 1,
                Code = 2,
                Flags = TransferFlags.None,
                Amount = 100
            };
        }

        var createTransferErrors = client.CreateTransfers(transfers);
        Assert.IsTrue(createTransferErrors.Length == 0);

        {
            // Querying transfers where:
            // `debit_account_id=$account1Id OR credit_account_id=$account1Id
            // ORDER BY timestamp ASC`.
            var filter = new GetAccountTransfers
            {
                AccountId = accounts[0].Id,
                Timestamp = 0,
                Limit = 8190,
                Flags = GetAccountTransfersFlags.Credits | GetAccountTransfersFlags.Debits
            };
            var account_transfers = client.GetAccountTransfers(filter);
            Assert.IsTrue(account_transfers.Length == 10);
            ulong timestamp = 0;
            foreach (var transfer in account_transfers)
            {
                Assert.IsTrue(transfer.Timestamp > timestamp);
                timestamp = transfer.Timestamp;
            }
        }

        {
            // Querying transfers where:
            // `debit_account_id=$account2Id OR credit_account_id=$account2Id
            // ORDER BY timestamp DESC`.
            var filter = new GetAccountTransfers
            {
                AccountId = accounts[1].Id,
                Timestamp = 0,
                Limit = 8190,
                Flags = GetAccountTransfersFlags.Credits | GetAccountTransfersFlags.Debits | GetAccountTransfersFlags.Reversed
            };
            var account_transfers = client.GetAccountTransfers(filter);
            Assert.IsTrue(account_transfers.Length == 10);
            ulong timestamp = ulong.MaxValue;
            foreach (var transfer in account_transfers)
            {
                Assert.IsTrue(transfer.Timestamp < timestamp);
                timestamp = transfer.Timestamp;
            }
        }

        {
            // Querying transfers where:
            // `debit_account_id=$account1Id
            // ORDER BY timestamp ASC`.                
            var filter = new GetAccountTransfers
            {
                AccountId = accounts[0].Id,
                Timestamp = 0,
                Limit = 8190,
                Flags = GetAccountTransfersFlags.Debits
            };
            var account_transfers = client.GetAccountTransfers(filter);
            Assert.IsTrue(account_transfers.Length == 5);
            ulong timestamp = 0;
            foreach (var transfer in account_transfers)
            {
                Assert.IsTrue(transfer.Timestamp > timestamp);
                timestamp = transfer.Timestamp;
            }
        }

        {
            // Querying transfers where:
            // `credit_account_id=$account2Id
            // ORDER BY timestamp DESC`.            
            var filter = new GetAccountTransfers
            {
                AccountId = accounts[1].Id,
                Timestamp = 0,
                Limit = 8190,
                Flags = GetAccountTransfersFlags.Credits | GetAccountTransfersFlags.Reversed
            };
            var account_transfers = client.GetAccountTransfers(filter);
            Assert.IsTrue(account_transfers.Length == 5);
            ulong timestamp = ulong.MaxValue;
            foreach (var transfer in account_transfers)
            {
                Assert.IsTrue(transfer.Timestamp < timestamp);
                timestamp = transfer.Timestamp;
            }
        }

        {
            // Querying transfers where:
            // `debit_account_id=$account1Id OR credit_account_id=$account1Id
            // ORDER BY timestamp ASC LIMIT 5`.       
            var filter = new GetAccountTransfers
            {
                AccountId = accounts[0].Id,
                Timestamp = 0,
                Limit = 5,
                Flags = GetAccountTransfersFlags.Credits | GetAccountTransfersFlags.Debits
            };

            // First 5 items:            
            var account_transfers = client.GetAccountTransfers(filter);
            Assert.IsTrue(account_transfers.Length == 5);
            ulong timestamp = 0;
            foreach (var transfer in account_transfers)
            {
                Assert.IsTrue(transfer.Timestamp > timestamp);
                timestamp = transfer.Timestamp;
            }

            // Next 5 items from this timestamp:
            filter.Timestamp = timestamp;
            account_transfers = client.GetAccountTransfers(filter);
            Assert.IsTrue(account_transfers.Length == 5);
            foreach (var transfer in account_transfers)
            {
                Assert.IsTrue(transfer.Timestamp > timestamp);
                timestamp = transfer.Timestamp;
            }

            // No more pages after that:
            filter.Timestamp = timestamp;
            account_transfers = client.GetAccountTransfers(filter);
            Assert.IsTrue(account_transfers.Length == 0);
        }

        {
            // Querying transfers where:
            // `debit_account_id=$account2Id OR credit_account_id=$account2Id
            // ORDER BY timestamp DESC LIMIT 5`.       
            var filter = new GetAccountTransfers
            {
                AccountId = accounts[1].Id,
                Timestamp = 0,
                Limit = 5,
                Flags = GetAccountTransfersFlags.Credits | GetAccountTransfersFlags.Debits | GetAccountTransfersFlags.Reversed
            };

            // First 5 items:                
            var account_transfers = client.GetAccountTransfers(filter);
            Assert.IsTrue(account_transfers.Length == 5);
            ulong timestamp = ulong.MaxValue;
            foreach (var transfer in account_transfers)
            {
                Assert.IsTrue(transfer.Timestamp < timestamp);
                timestamp = transfer.Timestamp;
            }

            // Next 5 items from this timestamp:
            filter.Timestamp = timestamp;
            account_transfers = client.GetAccountTransfers(filter);
            Assert.IsTrue(account_transfers.Length == 5);
            foreach (var transfer in account_transfers)
            {
                Assert.IsTrue(transfer.Timestamp < timestamp);
                timestamp = transfer.Timestamp;
            }

            // No more pages after that:
            filter.Timestamp = timestamp;
            account_transfers = client.GetAccountTransfers(filter);
            Assert.IsTrue(account_transfers.Length == 0);
        }

        {
            // Empty filter:
            Assert.IsTrue(client.GetAccountTransfers(new GetAccountTransfers { }).Length == 0);

            // Invalid account
            Assert.IsTrue(client.GetAccountTransfers(new GetAccountTransfers
            {
                AccountId = 0,
                Timestamp = 0,
                Limit = 8190,
                Flags = GetAccountTransfersFlags.Credits | GetAccountTransfersFlags.Debits,
            }).Length == 0);

            // Invalid timestamp
            Assert.IsTrue(client.GetAccountTransfers(new GetAccountTransfers
            {
                AccountId = accounts[0].Id,
                Timestamp = ulong.MaxValue,
                Limit = 8190,
                Flags = GetAccountTransfersFlags.Credits | GetAccountTransfersFlags.Debits,
            }).Length == 0);

            // Zero limit
            Assert.IsTrue(client.GetAccountTransfers(new GetAccountTransfers
            {
                AccountId = accounts[0].Id,
                Timestamp = 0,
                Limit = 0,
                Flags = GetAccountTransfersFlags.Credits | GetAccountTransfersFlags.Debits,
            }).Length == 0);

            // Empty flags
            Assert.IsTrue(client.GetAccountTransfers(new GetAccountTransfers
            {
                AccountId = accounts[0].Id,
                Timestamp = 0,
                Limit = 8190,
                Flags = (GetAccountTransfersFlags)0,
            }).Length == 0);

            // Invalid flags
            Assert.IsTrue(client.GetAccountTransfers(new GetAccountTransfers
            {
                AccountId = accounts[0].Id,
                Timestamp = 0,
                Limit = 8190,
                Flags = (GetAccountTransfersFlags)0xFFFF,
            }).Length == 0);
        }
    }

    /// <summary>
    /// This test asserts that a single Client can be shared by multiple concurrent tasks
    /// </summary>

    [TestMethod]
    [DoNotParallelize]
    public void ConcurrencyTest() => ConcurrencyTest(isAsync: false);

    [TestMethod]
    [DoNotParallelize]
    public void ConcurrencyTestAsync() => ConcurrencyTest(isAsync: true);

    private void ConcurrencyTest(bool isAsync)
    {
        const int TASKS_QTY = 32;
        int MAX_CONCURRENCY = 32;

        using var server = new TBServer();
        using var client = GetClient(server.Address, MAX_CONCURRENCY);

        var errors = client.CreateAccounts(accounts);
        Assert.IsTrue(errors.Length == 0);

        var list = new List<Task<CreateTransferResult>>();

        for (int i = 0; i < TASKS_QTY; i++)
        {
            var transfer = new Transfer
            {
                Id = (UInt128)(i + 1),
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Amount = 100,
                Ledger = 1,
                Code = 1,
            };

            /// Starts multiple tasks.
            var task = isAsync ? client.CreateTransferAsync(transfer) : Task.Run(() => client.CreateTransfer(transfer));
            list.Add(task);
        }

        Task.WhenAll(list).Wait();

        Assert.IsTrue(list.All(x => x.Result == CreateTransferResult.Ok));

        var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        // Assert that all tasks ran to the conclusion

        Assert.AreEqual(lookupAccounts[0].CreditsPosted, (ulong)(100 * TASKS_QTY));
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0LU);

        Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0LU);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, (ulong)(100 * TASKS_QTY));
    }

    /// <summary>
    /// This test asserts that a single Client can be shared by multiple concurrent tasks
    /// Even if a limited "concurrencyMax" value results in "ConcurrencyExceededException"
    /// </summary>

    [TestMethod]
    [DoNotParallelize]
    public void ConcurrencyExceededTest() => ConcurrencyExceededTest(isAsync: false);

    [TestMethod]
    [DoNotParallelize]
    public void ConcurrencyExceededTestAsync() => ConcurrencyExceededTest(isAsync: true);

    private void ConcurrencyExceededTest(bool isAsync)
    {
        const int TASKS_QTY = 32;
        int MAX_CONCURRENCY = 2;

        using var server = new TBServer();
        using var client = GetClient(server.Address, MAX_CONCURRENCY);

        var errors = client.CreateAccounts(accounts);
        Assert.IsTrue(errors.Length == 0);

        var list = new List<Task<CreateTransferResult>>();

        for (int i = 0; i < TASKS_QTY; i++)
        {
            var transfer = new Transfer
            {
                Id = (UInt128)(i + 1),
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
            };

            /// Starts multiple tasks using a client with a limited concurrencyMax:
            var task = isAsync ? client.CreateTransferAsync(transfer) : Task.Run(() => client.CreateTransfer(transfer));
            list.Add(task);
        }

        try
        {
            // Ignoring exceptions from the tasks.
            Task.WhenAll(list).Wait();
        }
        catch { }

        // It's expected for some tasks to failt with ConcurrencyExceededException or ObjectDisposedException:
        var successCount = list.Count(x => !x.IsFaulted && x.Result == CreateTransferResult.Ok);
        var failedCount = list.Count(x => x.IsFaulted &&
            (AssertException<ConcurrencyExceededException>(x.Exception!) ||
            AssertException<ObjectDisposedException>(x.Exception!)));
        Assert.IsTrue(successCount > 0);
        Assert.IsTrue(successCount + failedCount == TASKS_QTY);

        // Asserting that either the task failed or succeeded.
        Assert.IsTrue(list.All(x => x.IsFaulted || x.Result == CreateTransferResult.Ok));

        var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(lookupAccounts);

        // Assert that all tasks ran to the conclusion

        Assert.AreEqual(lookupAccounts[0].CreditsPosted, (ulong)(100 * successCount));
        Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0LU);

        Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0LU);
        Assert.AreEqual(lookupAccounts[1].DebitsPosted, (ulong)(100 * successCount));
    }

    /// <summary>
    /// This test asserts that Client.Dispose() will wait for any ongoing request to complete
    /// And new requests will fail with ObjectDisposedException.
    /// </summary>

    [TestMethod]
    [DoNotParallelize]
    public void ConcurrentTasksDispose() => ConcurrentTasksDispose(isAsync: false);

    [TestMethod]
    [DoNotParallelize]
    public void ConcurrentTasksDisposeAsync() => ConcurrentTasksDispose(isAsync: true);

    private void ConcurrentTasksDispose(bool isAsync)
    {
        const int TASKS_QTY = 32;
        int MAX_CONCURRENCY = 32;

        using var server = new TBServer();
        using var client = GetClient(server.Address, MAX_CONCURRENCY);

        var results = client.CreateAccounts(accounts);
        Assert.IsTrue(results.Length == 0);

        var list = new List<Task<CreateTransferResult>>();

        for (int i = 0; i < TASKS_QTY; i++)
        {
            var transfer = new Transfer
            {
                Id = (UInt128)(i + 1),
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Amount = 100,
                Ledger = 1,
                Code = 1,
            };

            /// Starts multiple tasks.
            var task = isAsync ? client.CreateTransferAsync(transfer) : Task.Run(() => client.CreateTransfer(transfer));
            list.Add(task);
        }

        // Waiting for just one task, the others may be pending.
        list.First().Wait();

        // Disposes the client, waiting all placed requests to finish.
        client.Dispose();

        try
        {
            // Ignoring exceptions from the tasks.
            Task.WhenAll(list).Wait();
        }
        catch { }

        // Asserting that either the task failed or succeeded,
        // at least one must be succeeded.
        Assert.IsTrue(list.Any(x => !x.IsFaulted && x.Result == CreateTransferResult.Ok));
        Assert.IsTrue(list.All(x => x.IsFaulted || x.Result == CreateTransferResult.Ok));
    }

    [TestMethod]
    [ExpectedException(typeof(ObjectDisposedException))]
    [DoNotParallelize]
    public void DisposedClient()
    {
        using var server = new TBServer();
        using var client = GetClient(server.Address);

        var accountResults = client.CreateAccounts(accounts);
        Assert.IsTrue(accountResults.Length == 0);

        client.Dispose();

        var transfer = new Transfer
        {
            Id = 1,
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Ledger = 1,
            Code = 1,
        };

        _ = client.CreateTransfers(new Transfer[] { transfer });
        Assert.IsTrue(false);
    }


    private static void AssertAccounts(Account[] lookupAccounts)
    {
        Assert.IsTrue(lookupAccounts.Length == accounts.Length);
        for (int i = 0; i < accounts.Length; i++)
        {
            AssertAccount(accounts[i], lookupAccounts[i]);
        }
    }

    private static void AssertAccount(Account a, Account b)
    {
        Assert.AreEqual(a.Id, b.Id);
        Assert.AreEqual(a.UserData128, b.UserData128);
        Assert.AreEqual(a.UserData64, b.UserData64);
        Assert.AreEqual(a.UserData32, b.UserData32);
        Assert.AreEqual(a.Flags, b.Flags);
        Assert.AreEqual(a.Code, b.Code);
        Assert.AreEqual(a.Ledger, b.Ledger);
    }

    private static void AssertTransfer(Transfer a, Transfer b)
    {
        Assert.AreEqual(a.Id, b.Id);
        Assert.AreEqual(a.DebitAccountId, b.DebitAccountId);
        Assert.AreEqual(a.CreditAccountId, b.CreditAccountId);
        Assert.AreEqual(a.Amount, b.Amount);
        Assert.AreEqual(a.PendingId, b.PendingId);
        Assert.AreEqual(a.UserData128, b.UserData128);
        Assert.AreEqual(a.UserData64, b.UserData64);
        Assert.AreEqual(a.UserData32, b.UserData32);
        Assert.AreEqual(a.Timeout, b.Timeout);
        Assert.AreEqual(a.Flags, b.Flags);
        Assert.AreEqual(a.Code, b.Code);
        Assert.AreEqual(a.Ledger, b.Ledger);
    }

    private static bool AssertException<T>(Exception exception) where T : Exception
    {
        while (exception is AggregateException aggregateException && aggregateException.InnerException != null)
        {
            exception = aggregateException.InnerException;
        }

        return exception is T;
    }
}

internal class TBServer : IDisposable
{
    // Path relative from /TigerBeetle.Test/bin/<framework>/<release>/<platform> :
    private const string PROJECT_ROOT = "../../../../..";
    private const string TB_PATH = PROJECT_ROOT + "/../../../zig-out/bin";
    private const string TB_EXE = "tigerbeetle";
    private const string TB_FILE = "dotnet-tests.tigerbeetle";
    private const string TB_SERVER = TB_PATH + "/" + TB_EXE;
    private const string FORMAT = $"format --cluster=0 --replica=0 --replica-count=1 ./" + TB_FILE;
    private const string START = $"start --addresses=0  --cache-grid=512MB ./" + TB_FILE;

    private readonly Process process;

    public string Address { get; }

    public TBServer()
    {
        CleanUp();

        {
            var format = new Process();
            format.StartInfo.FileName = TB_SERVER;
            format.StartInfo.Arguments = FORMAT;
            format.StartInfo.RedirectStandardError = true;
            format.Start();
            var formatStderr = format.StandardError.ReadToEnd();
            format.WaitForExit();
            if (format.ExitCode != 0) throw new InvalidOperationException($"format failed, ExitCode={format.ExitCode} stderr:\n{formatStderr}");
        }

        process = new Process();
        process.StartInfo.FileName = TB_SERVER;
        process.StartInfo.Arguments = START;
        process.StartInfo.RedirectStandardError = true;
        process.Start();
        if (process.WaitForExit(100)) throw new InvalidOperationException("Tigerbeetle server failed to start");

        var readPortEvent = new AutoResetEvent(false);
        var addressLogged = "";
        var stderrLogged = new StringBuilder();
        process.ErrorDataReceived += (sender, e) =>
        {
            var line = e.Data;
            if (line == null) return;
            stderrLogged.Append(line);
            const string listening = "listening on ";
            var found = line.IndexOf(listening);
            if (found != -1)
            {
                if (addressLogged != "") throw new InvalidOperationException("have already read the port");
                addressLogged = line.Substring(found + listening.Length).Trim();
                readPortEvent.Set();
            }
        };
        process.BeginErrorReadLine();
        readPortEvent.WaitOne(60_000);

        if (addressLogged == "")
        {
            process.Kill();
            process.WaitForExit();
            throw new InvalidOperationException($"failed to read the port, ExitCode={process.ExitCode} stderr:\n{stderrLogged.ToString()}");
        }

        process.CancelErrorRead();
        Address = addressLogged;
    }

    public void Dispose()
    {
        CleanUp();
    }

    private void CleanUp()
    {
        try
        {
            if (process != null && !process.HasExited)
            {
                process.Kill();
                process.Dispose();
            }

            File.Delete($"./{TB_FILE}");
        }
        catch { }
    }
}
