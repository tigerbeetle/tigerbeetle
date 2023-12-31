#pragma warning disable CS0162 // Unreachable code detected

using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace TigerBeetle.Benchmarks;

public static partial class Benchmark
{
    private const bool IS_ASYNC = false;
    private const int BATCHES_COUNT = 100;
    private const int MAX_MESSAGE_SIZE = (1024 * 1024) - 256; // config.message_size_max - @sizeOf(vsr.Header)
    private const int TRANSFERS_PER_BATCH = (MAX_MESSAGE_SIZE / Transfer.SIZE);
    private const int MAX_TRANSFERS = BATCHES_COUNT * TRANSFERS_PER_BATCH;

    public static async Task Main()
    {
        Console.WriteLine($"Benchmarking dotnet");

        using var client = new Client(0, new string[] { "3001" });

        var accounts = new[] {
                new Account
                {
                    Id = 1,
                    Code = 2,
                    Ledger = 777,
                },
                new Account
                {
                    Id = 2,
                    Code = 2,
                    Ledger = 777,
                }
            };

        var transfers = new Transfer[BATCHES_COUNT][];
        for (int i = 0; i < BATCHES_COUNT; i++)
        {
            transfers[i] = new Transfer[TRANSFERS_PER_BATCH];
            for (int j = 0; j < TRANSFERS_PER_BATCH; j++)
            {
                transfers[i][j] = new Transfer
                {
                    Id = new UInt128((ulong)i, (ulong)j + 1),
                    DebitAccountId = accounts[0].Id,
                    CreditAccountId = accounts[1].Id,
                    Amount = 1,
                    Code = 1,
                    Ledger = 777,
                };
            }
        }

        Console.WriteLine("creating accounts...");

        if (IS_ASYNC)
        {
            var results = await client.CreateAccountsAsync(accounts);
            if (results.Length > 0) throw new Exception("Invalid account results");
        }
        else
        {
            var results = client.CreateAccounts(accounts);
            if (results.Length > 0) throw new Exception("Invalid account results");
        }

        Console.WriteLine("starting benchmark...");

        var stopWatch = new Stopwatch();
        long totalTime = 0;
        long maxTransfersLatency = 0;

        for (int batchCount = 0; batchCount < BATCHES_COUNT; batchCount++)
        {
            var batch = transfers[batchCount];

            stopWatch.Restart();
            if (IS_ASYNC)
            {
                var ret = await client.CreateTransfersAsync(batch);
                if (ret.Length > 0) throw new Exception("Invalid transfer results");
            }
            else
            {
                var ret = client.CreateTransfers(batch);
                if (ret.Length > 0) throw new Exception("Invalid transfer results");
            }
            stopWatch.Stop();

            totalTime += stopWatch.ElapsedMilliseconds;
            maxTransfersLatency = Math.Max(stopWatch.ElapsedMilliseconds, maxTransfersLatency);
        }

        Console.WriteLine("============================================");

        var result = (long)((MAX_TRANSFERS * 1000) / totalTime);

        Console.WriteLine($"{result} transfers per second");
        Console.WriteLine($"create_transfers max p100 latency per {TRANSFERS_PER_BATCH} transfers = {maxTransfersLatency}ms");
        Console.WriteLine($"total {MAX_TRANSFERS} transfers in {totalTime}ms");
    }
}
