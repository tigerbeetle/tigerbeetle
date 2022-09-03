using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace TigerBeetle.Tests
{
	[TestClass]
	public class AccountTests
	{
		#region Fields

		private static Client GetClient(int maxConcurrency = 32) => new Client(0, new IPEndPoint[] { IPEndPoint.Parse($"127.0.0.1:{TBServer.TB_PORT}") }, maxConcurrency);

		private static readonly Account[] accounts = new[]
		{
			new Account
			{
				Id = Guid.NewGuid(),
				Ledger = 1,
				Code = 1,
			},
			new Account
			{
				Id = Guid.NewGuid(),
				Ledger = 1,
				Code = 2,
			}
		};

		#endregion Fields

		[TestMethod]
		[DoNotParallelize]
		public void CreateAccounts()
		{
			using (var server = new TBServer())
			{
				using var client = GetClient();

				var results = client.CreateAccounts(accounts);
				Assert.IsTrue(results.Length == 0);

				var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
				AssertAccounts(lookupAccounts);
			}
		}

		[TestMethod]
		[DoNotParallelize]
		public async Task CreateAccountsAsync()
		{
			using (var server = new TBServer())
			{
				using var client = GetClient();

				var results = await client.CreateAccountsAsync(accounts);
				Assert.IsTrue(results.Length == 0);

				var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
				AssertAccounts(lookupAccounts);
			}
		}

		private void AssertAccounts(Account[] lookupAccounts)
		{
			Assert.IsTrue(lookupAccounts.Length == 2);
			Assert.AreEqual(lookupAccounts[0].Id, accounts[0].Id);
			Assert.AreEqual(lookupAccounts[0].Code, accounts[0].Code);
			Assert.AreEqual(lookupAccounts[0].Ledger, accounts[0].Ledger);

			Assert.AreEqual(lookupAccounts[1].Id, accounts[1].Id);
			Assert.AreEqual(lookupAccounts[1].Code, accounts[1].Code);
			Assert.AreEqual(lookupAccounts[1].Ledger, accounts[1].Ledger);
		}

		[TestMethod]
		[DoNotParallelize]
		public void CreateTransfers()
		{
			using (var server = new TBServer())
			{
				using var client = GetClient();

				var results = client.CreateAccounts(accounts);
				Assert.IsTrue(results.Length == 0);

				var transfer = new Transfer
				{
					Id = Guid.NewGuid(),
					CreditAccountId = accounts[0].Id,
					DebitAccountId = accounts[1].Id,
					Ledger = 1,
					Code = 1,
					Amount = 100,
				};

				var result = client.CreateTransfer(transfer);
				Assert.IsTrue(result == CreateTransferResult.Ok);

				var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
				AssertAccounts(lookupAccounts);

				Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer.Amount);
				Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);

				Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
				Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer.Amount);
			}
		}

		[TestMethod]
		[DoNotParallelize]
		public async Task CreateTransfersAsync()
		{
			using (var server = new TBServer())
			{
				using var client = GetClient();

				var results = await client.CreateAccountsAsync(accounts);
				Assert.IsTrue(results.Length == 0);

				var transfer = new Transfer
				{
					Id = Guid.NewGuid(),
					CreditAccountId = accounts[0].Id,
					DebitAccountId = accounts[1].Id,
					Ledger = 1,
					Code = 1,
					Amount = 100,
				};

				var result = await client.CreateTransferAsync(transfer);
				Assert.IsTrue(result == CreateTransferResult.Ok);

				var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
				AssertAccounts(lookupAccounts);

				Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer.Amount);
				Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0u);

				Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0u);
				Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer.Amount);
			}
		}

		[TestMethod]
		[DoNotParallelize]
		public void CreatePendingTransfers()
		{
			using (var server = new TBServer())
			{
				using var client = GetClient();

				var results = client.CreateAccounts(accounts);
				Assert.IsTrue(results.Length == 0);

				var transfer = new Transfer
				{
					Id = Guid.NewGuid(),
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
					Id = Guid.NewGuid(),
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
		}

		[TestMethod]
		[DoNotParallelize]
		public async Task CreatePendingTransfersAsync()
		{
			using (var server = new TBServer())
			{
				using var client = GetClient();

				var results = await client.CreateAccountsAsync(accounts);
				Assert.IsTrue(results.Length == 0);

				var transfer = new Transfer
				{
					Id = Guid.NewGuid(),
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
					Id = Guid.NewGuid(),
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
		}

		[TestMethod]
		[DoNotParallelize]
		public void CreatePendingTransfersAndVoid()
		{
			using (var server = new TBServer())
			{
				using var client = GetClient();

				var results = client.CreateAccounts(accounts);
				Assert.IsTrue(results.Length == 0);

				var transfer = new Transfer
				{
					Id = Guid.NewGuid(),
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
					Id = Guid.NewGuid(),
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
		}

		[TestMethod]
		[DoNotParallelize]
		public async Task CreatePendingTransfersAndVoidAsync()
		{
			using (var server = new TBServer())
			{
				using var client = GetClient();

				var results = await client.CreateAccountsAsync(accounts);
				Assert.IsTrue(results.Length == 0);

				var transfer = new Transfer
				{
					Id = Guid.NewGuid(),
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
					Id = Guid.NewGuid(),
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
		}

		[TestMethod]
		[DoNotParallelize]
		public void CreateLikedTransfers()
		{
			using (var server = new TBServer())
			{
				using var client = GetClient();

				var results = client.CreateAccounts(accounts);
				Assert.IsTrue(results.Length == 0);

				var transfer1 = new Transfer
				{
					Id = Guid.NewGuid(),
					CreditAccountId = accounts[0].Id,
					DebitAccountId = accounts[1].Id,
					Ledger = 1,
					Code = 1,
					Amount = 100,
					Flags = TransferFlags.Linked,
				};

				var transfer2 = new Transfer
				{
					Id = Guid.NewGuid(),
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

				Assert.AreEqual(lookupAccounts[0].CreditsPending, 0u);
				Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer1.Amount);
				Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);
				Assert.AreEqual(lookupAccounts[0].DebitsPosted, transfer2.Amount);

				Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
				Assert.AreEqual(lookupAccounts[1].CreditsPosted, transfer2.Amount);
				Assert.AreEqual(lookupAccounts[1].DebitsPending, 0u);
				Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer1.Amount);
			}
		}

		[TestMethod]
		[DoNotParallelize]
		public async Task CreateLikedTransfersAsync()
		{
			using (var server = new TBServer())
			{
				using var client = GetClient();

				var results = await client.CreateAccountsAsync(accounts);
				Assert.IsTrue(results.Length == 0);

				var transfer1 = new Transfer
				{
					Id = Guid.NewGuid(),
					CreditAccountId = accounts[0].Id,
					DebitAccountId = accounts[1].Id,
					Ledger = 1,
					Code = 1,
					Amount = 100,
					Flags = TransferFlags.Linked,
				};

				var transfer2 = new Transfer
				{
					Id = Guid.NewGuid(),
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

				Assert.AreEqual(lookupAccounts[0].CreditsPending, 0u);
				Assert.AreEqual(lookupAccounts[0].CreditsPosted, transfer1.Amount);
				Assert.AreEqual(lookupAccounts[0].DebitsPending, 0u);
				Assert.AreEqual(lookupAccounts[0].DebitsPosted, transfer2.Amount);

				Assert.AreEqual(lookupAccounts[1].CreditsPending, 0u);
				Assert.AreEqual(lookupAccounts[1].CreditsPosted, transfer2.Amount);
				Assert.AreEqual(lookupAccounts[1].DebitsPending, 0u);
				Assert.AreEqual(lookupAccounts[1].DebitsPosted, transfer1.Amount);
			}
		}

		/// <summary>
		/// This test asserts that a single Client can be shared by multiple concurrent tasks
		/// Even if a limited "maxConcurrency" value forces new tasks to wait running tasks to complete
		/// </summary>

		[TestMethod]
		[DoNotParallelize]
		public void ConcurrentTasksTest()
		{
			const int TASKS_QTY = 12;
			int MAX_CONCURRENCY = new Random().Next(1, TASKS_QTY / 2);

			using (var server = new TBServer())
			{
				using var client = GetClient(MAX_CONCURRENCY);

				var results = client.CreateAccounts(accounts);
				Assert.IsTrue(results.Length == 0);

				var list = new List<Task<CreateTransferResult>>();

				for (int i = 0; i < TASKS_QTY; i++)
				{
					var transfer = new Transfer
					{
						Id = Guid.NewGuid(),
						CreditAccountId = accounts[0].Id,
						DebitAccountId = accounts[1].Id,
						Ledger = 1,
						Code = 1,
						Amount = 100,
					};

					/// Starts multiple tasks using a client with a limited maxConcurrency
					var task = Task.Run(() => client.CreateTransfer(transfer));
					list.Add(task);
				}

				Task.WaitAll(list.ToArray());
				Assert.IsTrue(list.All(x => x.Result == CreateTransferResult.Ok));

				var lookupAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
				AssertAccounts(lookupAccounts);

				// Assert that all tasks ran to the conclusion

				Assert.AreEqual(lookupAccounts[0].CreditsPosted, (ulong)(100 * TASKS_QTY));
				Assert.AreEqual(lookupAccounts[0].DebitsPosted, 0LU);

				Assert.AreEqual(lookupAccounts[1].CreditsPosted, 0LU);
				Assert.AreEqual(lookupAccounts[1].DebitsPosted, (ulong)(100 * TASKS_QTY));
			}
		}

		/// <summary>
		/// This test asserts that Client.Dispose() will wait for any ongoing request to complete
		/// And new requests will fail with ObjectDisposedException
		/// </summary>

		[TestMethod]
		[DoNotParallelize]
		public void ConcurrentTasksDispose()
		{
			const int TASKS_QTY = 12;
			int MAX_CONCURRENCY = new Random().Next(1, TASKS_QTY / 4);

			using (var server = new TBServer())
			{
				var client = GetClient(MAX_CONCURRENCY);

				var results = client.CreateAccounts(accounts);
				Assert.IsTrue(results.Length == 0);

				var list = new List<Task<CreateTransferResult>>();

				for (int i = 0; i < TASKS_QTY; i++)
				{
					var transfer = new Transfer
					{
						Id = Guid.NewGuid(),
						CreditAccountId = accounts[0].Id,
						DebitAccountId = accounts[1].Id,
						Ledger = 1,
						Code = 1,
						Amount = 100,
					};

					/// Starts multiple tasks using a client with a limited maxConcurrency
					var task = Task.Run(() => client.CreateTransfer(transfer));
					list.Add(task);
				}

				// Waiting for just one task
				list.First().Wait();

				// Disposes the client, forcing all tasks to finish if already submited a message, or fail
				client.Dispose();

				try
				{
					// Ignoring exceptions from the tasks
					Task.WhenAll(list).Wait();
				}
				catch { }

				// Asserting that either the task failed or succeeded
				Assert.IsTrue(list.All(x => x.IsFaulted || x.Result == CreateTransferResult.Ok));
			}
		}
	}

	internal class TBServer : IDisposable
	{
		#region Fields

		public const string TB_EXE = "tigerbeetle";
		public const string TB_PATH = "../../../../../tigerbeetle/";
		public static readonly string TB_SERVER = $"{TB_PATH}/{TB_EXE}";
		public const int TB_PORT = 3001;
		public const string TB_FILE = "dotnet-tests.tigerbeetle";
		public static readonly string FORMAT = $"format --cluster=0 --replica=0 ./{TB_FILE}";
		public static readonly string START = $"start --addresses={TB_PORT} ./{TB_FILE}";

		private Process process;

		#endregion Fields

		#region Constructor

		public TBServer()
		{
			CleanUp();

			var format = Process.Start(TB_SERVER, FORMAT);
			format.WaitForExit();
			if (format.ExitCode != 0) throw new InvalidOperationException("format failed");

			process = Process.Start(TB_SERVER, START);
			if (process.WaitForExit(100)) throw new InvalidOperationException("Tigerbeetle server failed to start");
		}

		#endregion Constructor

		#region Methods

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

		#endregion Methods
	}
}