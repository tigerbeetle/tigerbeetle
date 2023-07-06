using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace TigerBeetle.Tests
{
    [TestClass]
    public class IntegrationTests
    {
        private static Client GetClient(int concurrencyMax = 32) => new(0, new string[] { TBServer.TB_PORT }, concurrencyMax);

        private static readonly Account[] accounts = new[]
        {
            new Account
            {
                Id = 100,
                UserData = 1000,
                Flags = AccountFlags.None,
                Ledger = 1,
                Code = 1,
            },
            new Account
            {
                Id = 101,
                UserData = 1001,
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
        [ExpectedException(typeof(ArgumentException))]
        public void ConstructorWithZeroConcurrencyMax()
        {
            _ = new Client(0, new string[] { "3000" }, 0);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
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
            using var client = GetClient();

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
            using var client = GetClient();

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
            using var client = GetClient();

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
            using var client = GetClient();

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
            using var client = GetClient();

            var accountResults = client.CreateAccounts(accounts);
            Assert.IsTrue(accountResults.Length == 0);

            var transfer = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
            };

            var transferResults = client.CreateTransfers(new Transfer[] { transfer });
            Assert.IsTrue(transferResults.Length == 0);

            var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
            AssertAccounts(lookupAccounts);

            var lookupTransfers = client.LookupTransfers(new UInt128[] { transfer.Id });
            Assert.IsTrue(lookupTransfers.Length == 1);
            AssertTransfer(transfer, lookupTransfers[0]);

            Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer.Amount);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);

            Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer.Amount);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task CreateTransfersAsync()
        {
            using var server = new TBServer();
            using var client = GetClient();

            var accountResults = await client.CreateAccountsAsync(accounts);
            Assert.IsTrue(accountResults.Length == 0);

            var transfer = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
            };

            var transferResults = await client.CreateTransfersAsync(new Transfer[] { transfer });
            Assert.IsTrue(transferResults.Length == 0);

            var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
            AssertAccounts(lookupAccounts);

            var lookupTransfers = await client.LookupTransfersAsync(new UInt128[] { transfer.Id });
            Assert.IsTrue(lookupTransfers.Length == 1);
            AssertTransfer(transfer, lookupTransfers[0]);

            Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer.Amount);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);

            Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer.Amount);
        }

        [TestMethod]
        [DoNotParallelize]
        public void CreateTransfer()
        {
            using var server = new TBServer();
            using var client = GetClient();

            var results = client.CreateAccounts(accounts);
            Assert.IsTrue(results.Length == 0);

            var transfer = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
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
            using var client = GetClient();

            var results = await client.CreateAccountsAsync(accounts);
            Assert.IsTrue(results.Length == 0);

            var transfer = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
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
            using var client = GetClient();

            var results = client.CreateAccounts(accounts);
            Assert.IsTrue(results.Length == 0);

            var transfer = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
                Flags = TransferFlags.Pending,
                Timeout = int.MaxValue,
            };

            var result = client.CreateTransfer(transfer);
            Assert.IsTrue(result == CreateTransferResult.Ok);

            var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
            AssertAccounts(lookupAccounts);

            Assert.AreEqual(lookupAccounts[0].CreditsPending, transfer.Amount);
            Assert.AreEqual(lookupAccounts[0].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);

            Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPending, transfer.Amount);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, 0u);

            var postTransfer = new Transfer
            {
                Id = 2,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
                Flags = TransferFlags.PostPendingTransfer,
                PendingId = transfer.Id,
            };

            var postResult = client.CreateTransfer(postTransfer);
            Assert.IsTrue(postResult == CreateTransferResult.Ok);

            lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
            AssertAccounts(lookupAccounts);

            Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer.Amount);
            Assert.AreEqual(lookupAccounts[0].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);

            Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer.Amount);
            Assert.AreEqual(lookupAccounts[1].DebitsPending, 0u);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task CreatePendingTransfersAsync()
        {
            using var server = new TBServer();
            using var client = GetClient();

            var results = await client.CreateAccountsAsync(accounts);
            Assert.IsTrue(results.Length == 0);

            var transfer = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
                Flags = TransferFlags.Pending,
                Timeout = int.MaxValue,
            };

            var result = await client.CreateTransferAsync(transfer);
            Assert.IsTrue(result == CreateTransferResult.Ok);

            var lookupAccounts = await client.LookupAccountsAsync(new[] { accounts[0].Id, accounts[1].Id });
            AssertAccounts(lookupAccounts);

            Assert.AreEqual(lookupAccounts[0].CreditsPending, transfer.Amount);
            Assert.AreEqual(lookupAccounts[0].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);

            Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPending, transfer.Amount);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, 0u);

            var postTransfer = new Transfer
            {
                Id = 2,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
                Flags = TransferFlags.PostPendingTransfer,
                PendingId = transfer.Id,
            };

            var postResult = await client.CreateTransferAsync(postTransfer);
            Assert.IsTrue(postResult == CreateTransferResult.Ok);

            lookupAccounts = await client.LookupAccountsAsync(new[] { accounts[0].Id, accounts[1].Id });
            AssertAccounts(lookupAccounts);

            Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer.Amount);
            Assert.AreEqual(lookupAccounts[0].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);

            Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer.Amount);
            Assert.AreEqual(lookupAccounts[1].DebitsPending, 0u);
        }

        [TestMethod]
        [DoNotParallelize]
        public void CreatePendingTransfersAndVoid()
        {
            using var server = new TBServer();
            using var client = GetClient();

            var results = client.CreateAccounts(accounts);
            Assert.IsTrue(results.Length == 0);

            var transfer = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
                Flags = TransferFlags.Pending,
                Timeout = int.MaxValue,
            };

            var result = client.CreateTransfer(transfer);
            Assert.IsTrue(result == CreateTransferResult.Ok);

            var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
            AssertAccounts(lookupAccounts);

            Assert.AreEqual(lookupAccounts[0].CreditsPending, transfer.Amount);
            Assert.AreEqual(lookupAccounts[0].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);

            Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPending, transfer.Amount);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, 0u);

            var postTransfer = new Transfer
            {
                Id = 2,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
                Flags = TransferFlags.VoidPendingTransfer,
                PendingId = transfer.Id,
            };

            var postResult = client.CreateTransfer(postTransfer);
            Assert.IsTrue(postResult == CreateTransferResult.Ok);

            lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
            AssertAccounts(lookupAccounts);

            Assert.AreEqual(lookupAccounts[0].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[0].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);

            Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPending, 0u);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task CreatePendingTransfersAndVoidAsync()
        {
            using var server = new TBServer();
            using var client = GetClient();

            var results = await client.CreateAccountsAsync(accounts);
            Assert.IsTrue(results.Length == 0);

            var transfer = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
                Flags = TransferFlags.Pending,
                Timeout = int.MaxValue,
            };

            var result = await client.CreateTransferAsync(transfer);
            Assert.IsTrue(result == CreateTransferResult.Ok);

            var lookupAccounts = await client.LookupAccountsAsync(new[] { accounts[0].Id, accounts[1].Id });
            AssertAccounts(lookupAccounts);

            Assert.AreEqual(lookupAccounts[0].CreditsPending, transfer.Amount);
            Assert.AreEqual(lookupAccounts[0].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);

            Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPending, transfer.Amount);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, 0u);

            var postTransfer = new Transfer
            {
                Id = 2,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
                Flags = TransferFlags.VoidPendingTransfer,
                PendingId = transfer.Id,
            };

            var postResult = await client.CreateTransferAsync(postTransfer);
            Assert.IsTrue(postResult == CreateTransferResult.Ok);

            lookupAccounts = await client.LookupAccountsAsync(new[] { accounts[0].Id, accounts[1].Id });
            AssertAccounts(lookupAccounts);

            Assert.AreEqual(lookupAccounts[0].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[0].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);

            Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPending, 0u);
        }

        [TestMethod]
        [DoNotParallelize]
        public void CreateLinkedTransfers()
        {
            using var server = new TBServer();
            using var client = GetClient();

            var results = client.CreateAccounts(accounts);
            Assert.IsTrue(results.Length == 0);

            var transfer1 = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
                Flags = TransferFlags.Linked,
            };

            var transfer2 = new Transfer
            {
                Id = 2,
                CreditAccountId = accounts[1].Id,
                DebitAccountId = accounts[0].Id,
                Ledger = 1,
                Code = 1,
                Amount = 49,
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

            Assert.AreEqual(lookupAccounts[0].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer1.Amount);
            Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, transfer2.Amount);

            Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].CreditsPosted, transfer2.Amount);
            Assert.AreEqual(lookupAccounts[1].DebitsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer1.Amount);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task CreateLinkedTransfersAsync()
        {
            using var server = new TBServer();
            using var client = GetClient();

            var results = await client.CreateAccountsAsync(accounts);
            Assert.IsTrue(results.Length == 0);

            var transfer1 = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
                Flags = TransferFlags.Linked,
            };

            var transfer2 = new Transfer
            {
                Id = 2,
                CreditAccountId = accounts[1].Id,
                DebitAccountId = accounts[0].Id,
                Ledger = 1,
                Code = 1,
                Amount = 49,
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

            Assert.AreEqual(lookupAccounts[0].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer1.Amount);
            Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);
            Assert.AreEqual(lookupAccounts[0].DebitsPosted, transfer2.Amount);

            Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].CreditsPosted, transfer2.Amount);
            Assert.AreEqual(lookupAccounts[1].DebitsPending, 0u);
            Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer1.Amount);
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
            using var client = GetClient(MAX_CONCURRENCY);

            var errors = client.CreateAccounts(accounts);
            Assert.IsTrue(errors.Length == 0);

            var list = new List<Task<CreateTransferResult>>();

            for (int i = 0; i < TASKS_QTY; i++)
            {
                var transfer = new Transfer
                {
                    Id = i + 1,
                    CreditAccountId = accounts[0].Id,
                    DebitAccountId = accounts[1].Id,
                    Ledger = 1,
                    Code = 1,
                    Amount = 100,
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
            using var client = GetClient(MAX_CONCURRENCY);

            var errors = client.CreateAccounts(accounts);
            Assert.IsTrue(errors.Length == 0);

            var list = new List<Task<CreateTransferResult>>();

            for (int i = 0; i < TASKS_QTY; i++)
            {
                var transfer = new Transfer
                {
                    Id = i + 1,
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
            using var client = GetClient(MAX_CONCURRENCY);

            var results = client.CreateAccounts(accounts);
            Assert.IsTrue(results.Length == 0);

            var list = new List<Task<CreateTransferResult>>();

            for (int i = 0; i < TASKS_QTY; i++)
            {
                var transfer = new Transfer
                {
                    Id = i + 1,
                    CreditAccountId = accounts[0].Id,
                    DebitAccountId = accounts[1].Id,
                    Ledger = 1,
                    Code = 1,
                    Amount = 100,
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
            using var client = GetClient();

            var accountResults = client.CreateAccounts(accounts);
            Assert.IsTrue(accountResults.Length == 0);

            client.Dispose();

            var transfer = new Transfer
            {
                Id = 1,
                CreditAccountId = accounts[0].Id,
                DebitAccountId = accounts[1].Id,
                Ledger = 1,
                Code = 1,
                Amount = 100,
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
            Assert.AreEqual(a.UserData, b.UserData);
            Assert.AreEqual(a.Flags, b.Flags);
            Assert.AreEqual(a.Code, b.Code);
            Assert.AreEqual(a.Ledger, b.Ledger);
        }

        private static void AssertTransfer(Transfer a, Transfer b)
        {
            Assert.AreEqual(a.Id, b.Id);
            Assert.AreEqual(a.DebitAccountId, b.DebitAccountId);
            Assert.AreEqual(a.CreditAccountId, b.CreditAccountId);
            Assert.AreEqual(a.UserData, b.UserData);
            Assert.AreEqual(a.Flags, b.Flags);
            Assert.AreEqual(a.Code, b.Code);
            Assert.AreEqual(a.Ledger, b.Ledger);
            Assert.AreEqual(a.Amount, b.Amount);
            Assert.AreEqual(a.PendingId, b.PendingId);
            Assert.AreEqual(a.Timeout, b.Timeout);
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
        public const string TB_PORT = "3001";

        // Path relative from /TigerBeetle.Test/bin/<framework>/<release>/<platform> :
        private const string PROJECT_ROOT = "../../../../..";
        private const string TB_PATH = PROJECT_ROOT + "/../../../zig-out/bin";
        private const string TB_EXE = "tigerbeetle";
        private const string TB_FILE = "dotnet-tests.tigerbeetle";
        private const string TB_SERVER = TB_PATH + "/" + TB_EXE;
        private const string FORMAT = $"format --cluster=0 --replica=0 --replica-count=1 ./" + TB_FILE;
        private const string START = $"start --addresses=" + TB_PORT + " --cache-grid=128MB ./" + TB_FILE;

        private readonly Process process;

        public TBServer()
        {
            CleanUp();

            var format = Process.Start(TB_SERVER, FORMAT);
            format.WaitForExit();
            if (format.ExitCode != 0) throw new InvalidOperationException("format failed");

            process = Process.Start(TB_SERVER, START);
            if (process.WaitForExit(100)) throw new InvalidOperationException("Tigerbeetle server failed to start");
        }

        public void Dispose()
        {
            CleanUp();
        }

        private void CleanUp()
        {
            try
            {
                if (Process.GetProcessesByName(TB_EXE) is Process[] runningList)
                {
                    foreach (var runningProcess in runningList)
                    {
                        runningProcess.Kill();
                    }
                }
            }
            catch { }

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
}
