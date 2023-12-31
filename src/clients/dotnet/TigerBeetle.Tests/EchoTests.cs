using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace TigerBeetle.Tests;

[TestClass]
public class EchoTests
{
    private const int HEADER_SIZE = 256; // @sizeOf(vsr.Header)
    private const int MESSAGE_SIZE_MAX = 1024 * 1024; // config.message_size_max
    private static readonly int TRANSFER_SIZE = Marshal.SizeOf(typeof(Transfer));
    private static readonly int ITEMS_PER_BATCH = (MESSAGE_SIZE_MAX - HEADER_SIZE) / TRANSFER_SIZE;

    [TestMethod]
    public void Accounts()
    {
        var rnd = new Random(1);
        using var client = new EchoClient(0, new[] { "3000" }, 32);

        var batch = GetRandom<Account>(rnd);
        var reply = client.Echo(batch);
        Assert.IsTrue(batch.SequenceEqual(reply));
    }

    [TestMethod]
    public async Task AccountsAsync()
    {
        var rnd = new Random(2);
        using var client = new EchoClient(0, new[] { "3000" }, 32);

        var batch = GetRandom<Account>(rnd);
        var reply = await client.EchoAsync(batch);
        Assert.IsTrue(batch.SequenceEqual(reply));
    }

    [TestMethod]
    public void Transfers()
    {
        var rnd = new Random(3);
        using var client = new EchoClient(0, new[] { "3000" }, 32);

        var batch = GetRandom<Transfer>(rnd);
        var reply = client.Echo(batch);
        Assert.IsTrue(batch.SequenceEqual(reply));
    }

    [TestMethod]
    public async Task TransfersAsync()
    {
        var rnd = new Random(4);
        using var client = new EchoClient(0, new[] { "3000" }, 32);

        var batch = GetRandom<Transfer>(rnd);
        var reply = await client.EchoAsync(batch);
        Assert.IsTrue(batch.SequenceEqual(reply));
    }

    [TestMethod]
    public async Task ConcurrentAccountsAsync()
    {
        const int MAX_CONCURRENCY = 64;
        var rnd = new Random(5);
        using var client = new EchoClient(0, new[] { "3000" }, MAX_CONCURRENCY);

        const int MAX_REPETITIONS = 5;
        for (int repetition = 0; repetition < MAX_REPETITIONS; repetition++)
        {
            var list = new List<(Account[] batch, Task<Account[]> task)>();

            for (int i = 0; i < MAX_CONCURRENCY; i++)
            {
                var batch = GetRandom<Account>(rnd);
                var task = client.EchoAsync(batch);
                list.Add((batch, task));
            }

            foreach (var (batch, task) in list)
            {
                var reply = await task;
                Assert.IsTrue(batch.SequenceEqual(reply));
            }
        }
    }

    [TestMethod]
    public void ConcurrentTransfers()
    {
        const int MAX_CONCURRENCY = 64;
        var rnd = new Random(6);
        using var client = new EchoClient(0, new[] { "3000" }, MAX_CONCURRENCY);

        const int MAX_REPETITIONS = 5;
        for (int repetition = 0; repetition < MAX_REPETITIONS; repetition++)
        {
            var barrier = new Barrier(MAX_CONCURRENCY);
            var list = new List<ThreadContext>();

            for (int i = 0; i < MAX_CONCURRENCY; i++)
            {
                var batch = GetRandom<Transfer>(rnd);
                var threadContext = new ThreadContext(client, barrier, batch);
                list.Add(threadContext);
            }

            foreach (var item in list)
            {
                var reply = item.Wait();
                Assert.IsTrue(item.Batch.SequenceEqual(reply));
            }
        }
    }

    private class ThreadContext
    {
        private readonly Thread thread;
        private readonly Transfer[] batch;
        private Transfer[]? reply = null;
        private Exception? exception;

        public Transfer[] Batch => batch;

        public ThreadContext(EchoClient client, Barrier barrier, Transfer[] batch)
        {
            this.batch = batch;
            this.thread = new Thread(new ParameterizedThreadStart(Run));
            this.thread.Start((client, barrier));
        }

        private void Run(object? state)
        {
            try
            {
                var (client, barrier) = ((EchoClient, Barrier))state!;
                barrier.SignalAndWait();
                reply = client.Echo(batch);
            }
            catch (Exception exception)
            {
                this.exception = exception;
            }
        }

        public Transfer[] Wait()
        {
            thread.Join();
            return reply ?? throw exception!;
        }
    }

    private static T[] GetRandom<T>(Random rnd)
        where T : unmanaged
    {
        var size = rnd.Next(1, ITEMS_PER_BATCH);

        var buffer = new byte[size * Marshal.SizeOf(typeof(T))];
        rnd.NextBytes(buffer);
        return MemoryMarshal.Cast<byte, T>(buffer).ToArray();
    }
}
