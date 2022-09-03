#pragma warning disable CS0162 // Unreachable code detected

using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace TigerBeetle.Benchmarks
{
	public static partial class Benchmark
	{
		#region Fields

		private const bool IS_ASYNC = true;
		private const int BATCHES_COUNT = 3;
		private const int MAX_MESSAGE_SIZE = (1024 * 1024) - 128; // config.message_size_max - @sizeOf(vsr.Header)
		private const int TRANSFERS_PER_BATCH = (MAX_MESSAGE_SIZE / Transfer.SIZE);
		private const int MAX_TRANSFERS = BATCHES_COUNT * TRANSFERS_PER_BATCH;

		#endregion Fields

		#region Methods

		public static async Task Main()
		{
			Console.WriteLine($"Benchmarking dotnet");

			var queue = new TimedQueue();
			using var client = new Client(0, new IPEndPoint[] { IPEndPoint.Parse("127.0.0.1:3001") }, maxConcurrency: 45);

			var accounts = new[] {
				new Account
				{
					Id = 1,
					Code = 2,
					Ledger = ISO4217.ZAR.Code,
				},
				new Account
				{
					Id = 2,
					Code = 2,
					Ledger = ISO4217.ZAR.Code,
				}
			};

			// Pre-allocate a million transfers:
			var transfers = new Transfer[MAX_TRANSFERS];
			for (int i = 0; i < transfers.Length; i++)
			{
				transfers[i] = new Transfer
				{
					Id = i + 1,
					DebitAccountId = accounts[0].Id,
					CreditAccountId = accounts[1].Id,
					Code = 1,
					Ledger = ISO4217.ZAR.Code,
					Amount = ISO4217.ZAR.ToUInt64(0.01M),
				};
			}

			Console.WriteLine("creating accounts...");

			if (IS_ASYNC)
			{
				async Task createAccountsAsync()
				{
					var results = await client.CreateAccountsAsync(accounts);
					if (results.Any(x => x.Result != CreateAccountResult.Ok && x.Result != CreateAccountResult.Exists)) throw new Exception("Invalid account results");
				}

				queue.Batches.Enqueue(createAccountsAsync);
				await queue.ExecuteAsync();
			}
			else
			{
				void createAccounts()
				{
					var results = client.CreateAccounts(accounts);
					if (results.Any(x => x.Result != CreateAccountResult.Ok && x.Result != CreateAccountResult.Exists)) throw new Exception("Invalid account results");
				}

				queue.Batches.Enqueue(createAccounts);
				queue.Execute();
			}

			Trace.Assert(queue.Batches.Count == 0);

			Console.WriteLine("batching transfers...");
			queue.Reset();

			int batchCount = 0;
			int count = 0;

			for (; ; )
			{
				var batch = transfers.Skip(batchCount * TRANSFERS_PER_BATCH).Take(TRANSFERS_PER_BATCH).ToArray();
				if (batch.Length == 0) break;

				if (IS_ASYNC)
				{
					async Task createTransfersAsync()
					{
						var ret = await client.CreateTransfersAsync(batch);
						if (ret.Length > 0) throw new Exception("Invalid transfer results");
					}
					queue.Batches.Enqueue(createTransfersAsync);
				}
				else
				{
					void createTransfers()
					{
						var ret = client.CreateTransfers(batch);
						if (ret.Length > 0) throw new Exception("Invalid transfer results");
					}

					queue.Batches.Enqueue(createTransfers);
				}

				batchCount += 1;
				count += TRANSFERS_PER_BATCH;
			}

			Trace.Assert(count == MAX_TRANSFERS);

			Console.WriteLine("starting benchmark...");

			if (IS_ASYNC)
			{
				await queue.ExecuteAsync();
			}
			else
			{
				queue.Execute();
			}

			Trace.Assert(queue.Batches.Count == 0);

			var assertAccounts = client.LookupAccounts(new[] { accounts[0].Id, accounts[1].Id });
			Debug.Assert(accounts[0].Id == assertAccounts[0].Id);
			Debug.Assert(assertAccounts[0].DebitsPosted == (ulong)count);
			Debug.Assert(accounts[1].Id == assertAccounts[1].Id);
			Debug.Assert(assertAccounts[1].CreditsPosted == (ulong)count);

			var randonTransfer = transfers[1];
			var mayAssertTransfer = client.LookupTransfer(randonTransfer.Id);
			if (mayAssertTransfer is Transfer assertTransfer)
			{
				Debug.Assert(assertTransfer.Id == randonTransfer.Id);
				Debug.Assert(assertTransfer.Ledger == randonTransfer.Ledger);
				Debug.Assert(assertTransfer.Amount == randonTransfer.Amount);
				Debug.Assert(assertTransfer.CreditAccountId == randonTransfer.CreditAccountId);
				Debug.Assert(assertTransfer.DebitAccountId == randonTransfer.DebitAccountId);
			}
			else
			{
				Debug.Assert(false);
			}

			Console.WriteLine("============================================");

			var result = (long)((transfers.Length * 1000) / queue.TotalTime);

			Console.WriteLine($"{result} transfers per second");
			Console.WriteLine($"create_transfers max p100 latency per {TRANSFERS_PER_BATCH} transfers = {queue.MaxTransfersLatency}ms");
			Console.WriteLine($"total {transfers.Length} transfers in {queue.TotalTime}ms");
		}

		#endregion Methods
	}
}