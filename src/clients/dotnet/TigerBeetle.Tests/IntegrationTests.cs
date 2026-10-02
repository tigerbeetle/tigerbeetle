using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace TigerBeetle.Tests;

[TestClass]
public class IntegrationTests
{
    private static Account[] GenerateAccounts() => new[]
    {
            new Account
            {
                Id = ID.Create(),
                UserData128 = 1000,
                UserData64 = 1001,
                UserData32 = 1002,
                Flags = AccountFlags.None,
                Ledger = 1,
                Code = 1,
            },
            new Account
            {
                Id = ID.Create(),
                UserData128 = 1000,
                UserData64 = 1001,
                UserData32 = 1002,
                Flags = AccountFlags.None,
                Ledger = 1,
                Code = 2,
            },
    };

    // Created by the test initializer:
    private static TBServer server = null!;
    private static Client client = null!;

    [ClassInitialize]
    public static void Initialize(TestContext _)
    {
        server = new TBServer();
        client = new Client(0, server.Address);
    }

    [ClassCleanup]
    public static void Cleanup()
    {
        client.Dispose();
        server.Dispose();
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentNullException))]
    public void ConstructorWithNullReplicaAddresses()
    {
        string? addresses = null;
        _ = new Client(0, addresses!);
    }

    [TestMethod]
    public void ConstructorWithEmptyReplicaAddresses()
    {
        try
        {
            _ = new Client(0, "");
            Assert.Fail();
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
            _ = new Client(0, ",");
            Assert.Fail();
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
            var addresses = string.Join(",", Enumerable.Range(3000, 3100));
            _ = new Client(0, addresses);
            Assert.Fail();
        }
        catch (InitializationException exception)
        {
            Assert.AreEqual(InitializationStatus.AddressLimitExceeded, exception.Status);
        }
    }

    [TestMethod]
    public void ConstructorAndFinalizer()
    {
        // No using here, we want to test the finalizer
        var client = new Client(1, "3000");
        Assert.IsTrue(client.ClusterID == 1);
    }

    [TestMethod]
    [ExpectedException(typeof(OverflowException))]
    public void CreateAccountBatchSizeOverflow()
    {
        var batch = new DummyMemory<Account>(int.MaxValue);
        _ = client.CreateAccounts(batch.Memory.Span);
        Assert.Fail();
    }

    [TestMethod]
    [ExpectedException(typeof(OverflowException))]
    public async Task CreateAccountBatchSizeOverflowAsync()
    {
        var batch = new DummyMemory<Account>(int.MaxValue);
        _ = await client.CreateAccountsAsync(batch.Memory);
        Assert.Fail();
    }

    [TestMethod]
    public void CreateLinkedTransfers()
    {
        var accounts = GenerateAccounts();
        var accountResults = client.CreateAccounts(accounts);
        Assert.AreEqual(accounts.Length, accountResults.Length);
        Assert.IsTrue(accountResults.All(x => x.Timestamp > 0));
        Assert.IsTrue(accountResults.All(x => x.Status == CreateAccountStatus.Created));

        var transfer1 = new Transfer
        {
            Id = ID.Create(),
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Linked,
        };

        var transfer2 = new Transfer
        {
            Id = ID.Create(),
            CreditAccountId = accounts[1].Id,
            DebitAccountId = accounts[0].Id,
            Amount = 49,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.None,
        };

        var transferResults = client.CreateTransfers(new[] { transfer1, transfer2 });
        Assert.AreEqual(2, transferResults.Length);
        Assert.IsTrue(transferResults.All(x => x.Timestamp > 0));
        Assert.IsTrue(transferResults.All(x => x.Status == CreateTransferStatus.Created));

        var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(accounts, lookupAccounts);

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
    public async Task CreateLinkedTransfersAsync()
    {
        var accounts = GenerateAccounts();
        var accountResults = client.CreateAccounts(accounts);
        Assert.AreEqual(accounts.Length, accountResults.Length);
        Assert.IsTrue(accountResults.All(x => x.Timestamp > 0));
        Assert.IsTrue(accountResults.All(x => x.Status == CreateAccountStatus.Created));

        var transfer1 = new Transfer
        {
            Id = ID.Create(),
            CreditAccountId = accounts[0].Id,
            DebitAccountId = accounts[1].Id,
            Amount = 100,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.Linked,
        };

        var transfer2 = new Transfer
        {
            Id = ID.Create(),
            CreditAccountId = accounts[1].Id,
            DebitAccountId = accounts[0].Id,
            Amount = 49,
            Ledger = 1,
            Code = 1,
            Flags = TransferFlags.None,
        };

        var transferResults = await client.CreateTransfersAsync(new[] { transfer1, transfer2 });
        Assert.AreEqual(2, transferResults.Length);
        Assert.IsTrue(transferResults.All(x => x.Timestamp > 0));
        Assert.IsTrue(transferResults.All(x => x.Status == CreateTransferStatus.Created));

        var lookupAccounts = await client.LookupAccountsAsync(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(accounts, lookupAccounts);

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
    public void CreateAccountTooMuchData()
    {
        const int TOO_MUCH_DATA = 10_000;
        var accounts = new Account[TOO_MUCH_DATA];
        for (int i = 0; i < TOO_MUCH_DATA; i++)
        {
            accounts[i] = new Account
            {
                Id = ID.Create(),
                Code = 1,
                Ledger = 1
            };
        }
        Assert.ThrowsException<TooMuchDataException>(() => _ = client.CreateAccounts(accounts));
    }

    [TestMethod]

    public async Task CreateAccountTooMuchDataAsync()
    {
        const int TOO_MUCH_DATA = 10_000;
        var accounts = new Account[TOO_MUCH_DATA];
        for (int i = 0; i < TOO_MUCH_DATA; i++)
        {
            accounts[i] = new Account
            {
                Id = ID.Create(),
                Code = 1,
                Ledger = 1
            };
        }
        await Assert.ThrowsExceptionAsync<TooMuchDataException>(() => client.CreateAccountsAsync(accounts));
    }

    [TestMethod]
    public void CreateTransferTooMuchData()
    {
        const int TOO_MUCH_DATA = 10_000;
        var transfers = new Transfer[TOO_MUCH_DATA];
        for (int i = 0; i < TOO_MUCH_DATA; i++)
        {
            transfers[i] = new Transfer
            {
                Id = ID.Create(),
                Code = 1,
                Ledger = 1
            };
        }
        Assert.ThrowsException<TooMuchDataException>(() => _ = client.CreateTransfers(transfers));
    }

    [TestMethod]
    public async Task CreateTransferTooMuchDataAsync()
    {
        const int TOO_MUCH_DATA = 10_000;
        var transfers = new Transfer[TOO_MUCH_DATA];
        for (int i = 0; i < TOO_MUCH_DATA; i++)
        {
            transfers[i] = new Transfer
            {
                Id = ID.Create(),
                DebitAccountId = 1,
                CreditAccountId = 2,
                Code = 1,
                Ledger = 1,
                Amount = 100,
            };
        }
        await Assert.ThrowsExceptionAsync<TooMuchDataException>(() => client.CreateTransfersAsync(transfers));
    }

    [TestMethod]
    public void TestGetAccountTransfers()
    {
        var accounts = GenerateAccounts();
        accounts[0].Flags |= AccountFlags.History;
        accounts[1].Flags |= AccountFlags.History;
        var accountResults = client.CreateAccounts(accounts);
        Assert.AreEqual(accounts.Length, accountResults.Length);
        Assert.IsTrue(accountResults.All(x => x.Timestamp > 0));
        Assert.IsTrue(accountResults.All(x => x.Status == CreateAccountStatus.Created));

        // Creating a transfer.
        var transfers = new Transfer[10];
        for (int i = 0; i < 10; i++)
        {
            transfers[i] = new Transfer
            {
                Id = ID.Create(),

                // Swap the debit and credit accounts:
                CreditAccountId = i % 2 == 0 ? accounts[0].Id : accounts[1].Id,
                DebitAccountId = i % 2 == 0 ? accounts[1].Id : accounts[0].Id,

                Ledger = 1,
                Code = 2,
                Flags = TransferFlags.None,
                Amount = 100
            };
        }

        var transferResults = client.CreateTransfers(transfers);
        Assert.AreEqual(transfers.Length, transferResults.Length);
        Assert.IsTrue(transferResults.All(x => x.Timestamp > 0));
        Assert.IsTrue(transferResults.All(x => x.Status == CreateTransferStatus.Created));

        {
            // Invalid flags
            var filter = new AccountFilter
            {
                AccountId = accounts[0].Id,
                TimestampMin = 0,
                TimestampMax = 0,
                Limit = 254,
                Flags = (AccountFilterFlags)0xFFFF,
            };
            Assert.IsTrue(client.GetAccountTransfers(filter).Length == 0);
            Assert.IsTrue(client.GetAccountBalances(filter).Length == 0);
        }
    }

    [TestMethod]
    public void TestInvalidQueryFilter()
    {
        {
            // Invalid flags
            var filter = new QueryFilter
            {
                TimestampMin = 0,
                TimestampMax = 0,
                Limit = 254,
                Flags = (QueryFilterFlags)0xFFFF,
            };
            Assert.IsTrue(client.QueryAccounts(filter).Length == 0);
            Assert.IsTrue(client.QueryTransfers(filter).Length == 0);
        }
    }

    /// <summary>
    /// This test asserts that a single Client can be shared by multiple concurrent tasks
    /// </summary>

    [TestMethod]
    public void ConcurrencyTest() => ConcurrencyTest(isAsync: false);

    [TestMethod]
    public void ConcurrencyTestAsync() => ConcurrencyTest(isAsync: true);

    private void ConcurrencyTest(bool isAsync)
    {
        using var client = new Client(0, server.Address);

        var accounts = GenerateAccounts();
        var accountResults = client.CreateAccounts(accounts);
        Assert.AreEqual(accounts.Length, accountResults.Length);
        Assert.IsTrue(accountResults.All(x => x.Timestamp > 0));
        Assert.IsTrue(accountResults.All(x => x.Status == CreateAccountStatus.Created));

        var tasks = new Task[isAsync ? 1_000_000 : 10_000];
        for (int i = 0; i < tasks.Length; i += 2)
        {
            var transfer = new Transfer
            {
                Id = ID.Create(),
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Amount = 1,
                Ledger = 1,
                Code = 1,
            };

            // Starting two async requests of different operations.
            if (isAsync)
            {
                tasks[i] = client.CreateTransfersAsync(new[] { transfer });
                tasks[i + 1] = client.LookupAccountsAsync(new[] { accounts[0].Id });
            }
            else
            {
                tasks[i] = Task.Run(() => client.CreateTransfers(new[] { transfer }));
                tasks[i + 1] = Task.Run(() => client.LookupAccounts(new[] { accounts[0].Id }));
            }
        }
        Task.WhenAll(tasks).Wait();

        foreach (var task in tasks)
        {
            switch (task)
            {
                case Task<CreateTransferResult[]> createAccounts:
                    Assert.AreEqual(1, createAccounts.Result.Length);
                    Assert.IsTrue(createAccounts.Result[0].Timestamp > 0);
                    Assert.AreEqual(CreateTransferStatus.Created, createAccounts.Result[0].Status);
                    break;
                case Task<Account[]> lookupAccounts:
                    Assert.AreEqual(1, lookupAccounts.Result.Length);
                    Assert.AreEqual(accounts[0].Id, lookupAccounts.Result[0].Id);
                    break;
                default:
                    Assert.Fail();
                    break;
            }
        }

        var lookupResult = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
        AssertAccounts(accounts, lookupResult);

        // Assert that all tasks ran to the conclusion

        Assert.AreEqual(lookupResult[0].CreditsPosted, (uint)tasks.Length / 2);
        Assert.AreEqual(lookupResult[0].DebitsPosted, 0LU);

        Assert.AreEqual(lookupResult[1].CreditsPosted, 0LU);
        Assert.AreEqual(lookupResult[1].DebitsPosted, (uint)tasks.Length / 2);
    }

    /// <summary>
    /// This test asserts that a linked chain is consistent across concurrent requests.
    /// </summary>

    [TestMethod]
    public void ConcurrentLinkedChainTest() => ConcurrentLinkedChainTest(isAsync: false);

    [TestMethod]
    public void ConcurrentLinkedChainTestAsync() => ConcurrentLinkedChainTest(isAsync: true);

    private void ConcurrentLinkedChainTest(bool isAsync)
    {
        const int TASKS_QTY = 10_000;

        using var client = new Client(0, server.Address);

        var accounts = GenerateAccounts();
        var accountResults = client.CreateAccounts(accounts);
        Assert.AreEqual(accounts.Length, accountResults.Length);
        Assert.IsTrue(accountResults.All(x => x.Timestamp > 0));
        Assert.IsTrue(accountResults.All(x => x.Status == CreateAccountStatus.Created));

        var tasks = new Task<CreateTransferResult[]>[TASKS_QTY];

        async Task<CreateTransferResult[]> asyncAction(Transfer[] transfers)
        {
            return await client.CreateTransfersAsync(transfers);
        }

        CreateTransferResult[] syncAction(Transfer[] transfers)
        {
            return client.CreateTransfers(transfers);
        }

        for (int i = 0; i < TASKS_QTY; i++)
        {
            // The Linked flag will cause the
            // batch to fail due to LinkedEventChainOpen.
            var flags = i % 10 == 0 ? TransferFlags.Linked : TransferFlags.None;
            var transfers = new Transfer[] {
                new()
                {
                    Id = ID.Create(),
                    CreditAccountId = accounts[0].Id,
                    DebitAccountId = accounts[1].Id,
                    Amount = 1,
                    Ledger = 1,
                    Code = 1,
                    Flags = flags
                },
            };

            // Starts multiple requests.
            // Wraps the syncAction into a Task for unified logic handling both async and sync tests.
            tasks[i] = isAsync ? asyncAction(transfers) : Task.Run(() => syncAction(transfers));
        }

        Task.WhenAll(tasks).Wait();

        for (int i = 0; i < tasks.Length; i++)
        {
            CreateTransferResult[] results = tasks[i].Result;
            Assert.AreEqual(1, results.Length);

            if (i % 10 == 0)
            {
                Assert.AreEqual(results[0].Status, CreateTransferStatus.LinkedEventChainOpen);
            }
            else
            {
                Assert.AreEqual(results[0].Status, CreateTransferStatus.Created);
            }
        }
    }

    /// <summary>
    /// This test asserts that Client.Dispose() will wait for any ongoing request to complete
    /// And new requests will fail with ObjectDisposedException.
    /// </summary>

    [TestMethod]
    public void ConcurrentTasksDispose() => ConcurrentTasksDispose(isAsync: false);

    [TestMethod]
    public void ConcurrentTasksDisposeAsync() => ConcurrentTasksDispose(isAsync: true);

    private void ConcurrentTasksDispose(bool isAsync)
    {
        const int TASKS_QTY = 32;

        using var client = new Client(0, server.Address);

        var accounts = GenerateAccounts();
        var accountResults = client.CreateAccounts(accounts);
        Assert.AreEqual(accounts.Length, accountResults.Length);
        Assert.IsTrue(accountResults.All(x => x.Timestamp > 0));
        Assert.IsTrue(accountResults.All(x => x.Status == CreateAccountStatus.Created));

        var tasks = new Task<CreateTransferResult[]>[TASKS_QTY];

        for (int i = 0; i < TASKS_QTY; i++)
        {
            var transfers = new Transfer[]
            {
                new()
                {
                    Id = ID.Create(),
                    CreditAccountId = accounts[0].Id,
                    DebitAccountId = accounts[1].Id,
                    Amount = 100,
                    Ledger = 1,
                    Code = 1,
                },
            };

            /// Starts multiple tasks.
            var task = isAsync ? client.CreateTransfersAsync(transfers) : Task.Run(() => client.CreateTransfers(transfers));
            tasks[i] = task;
        }

        // Waiting for just one task, the others may be pending.
        Task.WaitAny(tasks);

        // Disposes the client, waiting all placed requests to finish.
        client.Dispose();

        try
        {
            // Ignoring exceptions from the tasks.
            Task.WhenAll(tasks).Wait();
        }
        catch { }

        // Asserting that either the task failed or succeeded,
        // at least one must be succeeded.
        Assert.IsTrue(tasks.Any(x => !x.IsFaulted && x.Result[0].Status == CreateTransferStatus.Created));
        Assert.IsTrue(tasks.All(x => x.IsFaulted || x.Result[0].Status == CreateTransferStatus.Created));
    }

    private static void AssertAccounts(Account[] expected, Account[] actual)
    {
        Assert.AreEqual(expected.Length, actual.Length);
        for (int i = 0; i < actual.Length; i++)
        {
            AssertAccount(actual[i], expected[i]);
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
    private readonly Process process;
    private readonly string dataFile;

    public string Address { get; }

    public TBServer()
    {
        dataFile = Path.GetRandomFileName();
        var tigerbeetleBinary = Environment.GetEnvironmentVariable("TIGERBEETLE_BINARY");
        if (tigerbeetleBinary == null)
        {
            throw new InvalidOperationException("TIGERBEETLE_BINARY environmental variable is required");
        }

        {
            using var format = new Process();
            format.StartInfo.FileName = tigerbeetleBinary;
            format.StartInfo.Arguments = $"format --cluster=0 --replica=0 --replica-count=1 --development ./{dataFile}";
            format.StartInfo.RedirectStandardError = true;
            format.Start();
            var formatStderr = format.StandardError.ReadToEnd();
            format.WaitForExit();
            if (format.ExitCode != 0) throw new InvalidOperationException($"format failed, ExitCode={format.ExitCode} stderr:\n{formatStderr}");
        }

        process = new Process();
        process.StartInfo.FileName = tigerbeetleBinary;
        process.StartInfo.Arguments = $"start --addresses=0 --development ./{dataFile}";
        process.StartInfo.RedirectStandardInput = true;
        process.StartInfo.RedirectStandardOutput = true;
        process.Start();

        Address = process.StandardOutput.ReadLine()!.Trim();
    }

    public void Dispose()
    {
        process.Kill();
        process.WaitForExit();
        process.Dispose();
        File.Delete($"./{dataFile}");
    }
}

/// <summary>
/// Dummy allocator capable of creating memory regions
/// and spans for testing purposes.
/// The contents cannot be dereferenced.
/// </summary>
sealed class DummyMemory<T> : MemoryManager<T>
    where T : unmanaged
{
    private readonly int length;

    public DummyMemory(int length)
    {
        this.length = length;
    }

    public override Memory<T> Memory => base.CreateMemory(length);

    public override Span<T> GetSpan()
    {
        unsafe
        {
            return new Span<T>(null, length);
        }
    }

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        return new MemoryHandle();
    }

    public override void Unpin()
    {
    }

    protected override void Dispose(bool disposing)
    {
        _ = disposing;
    }
}
