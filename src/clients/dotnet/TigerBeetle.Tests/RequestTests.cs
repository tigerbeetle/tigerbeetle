using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace TigerBeetle.Tests;

[TestClass]
public class RequestTests
{
    [TestMethod]
    [ExpectedException(typeof(ArgumentException))]
    public void EmptyBatch()
    {
        using var nativeClient = NativeClient.InitEcho(0, new string[] { "3000" }, 1);
        var request = new AsyncRequest<Account, UInt128>(nativeClient, TBOperation.LookupAccounts);

        request.Submit(Array.Empty<UInt128>());
        Assert.IsTrue(false);
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentNullException))]
    public void NullBatch()
    {
        using var nativeClient = NativeClient.InitEcho(0, new string[] { "3000" }, 1);
        var request = new AsyncRequest<Account, UInt128>(nativeClient, TBOperation.LookupAccounts);

        request.Submit((UInt128[])null!);
        Assert.IsTrue(false);
    }

    [TestMethod]
    [ExpectedException(typeof(AssertionException))]
    public async Task UnexpectedOperation()
    {
        using var nativeClient = NativeClient.InitEcho(0, new string[] { "3000" }, 1);

        var callback = new CallbackSimulator<Account, UInt128>(
            nativeClient,
            TBOperation.LookupAccounts,
            (byte)99,
            null,
            PacketStatus.Ok,
            delay: 100,
            isAsync: true
        );
        var task = callback.Run();
        Assert.IsFalse(task.IsCompleted);

        _ = await task;
        Assert.IsTrue(false);
    }

    [TestMethod]
    [ExpectedException(typeof(AssertionException))]
    public async Task InvalidSizeOperation()
    {
        using var nativeClient = NativeClient.InitEcho(0, new string[] { "3000" }, 1);

        var buffer = new byte[Account.SIZE + 1];
        var callback = new CallbackSimulator<Account, UInt128>(
            nativeClient,
            TBOperation.LookupAccounts,
            (byte)TBOperation.LookupAccounts,
            buffer,
            PacketStatus.Ok,
            delay: 100,
            isAsync: true
        );

        var task = callback.Run();
        Assert.IsFalse(task.IsCompleted);

        _ = await task;
        Assert.IsTrue(false);
    }

    [TestMethod]
    public async Task RequestException()
    {
        using var nativeClient = NativeClient.InitEcho(0, new string[] { "3000" }, 1);

        foreach (var isAsync in new bool[] { true, false })
        {
            var buffer = new byte[Account.SIZE];
            var callback = new CallbackSimulator<Account, UInt128>(nativeClient,
                TBOperation.LookupAccounts,
                (byte)TBOperation.LookupAccounts,
                buffer,
                PacketStatus.TooMuchData,
                delay: 100,
                isAsync
            );

            var task = callback.Run();
            Assert.IsFalse(task.IsCompleted);

            try
            {
                _ = await task;
                Assert.IsTrue(false);
            }
            catch (RequestException exception)
            {
                Assert.AreEqual(PacketStatus.TooMuchData, exception.Status);
            }
        }
    }

    [TestMethod]
    public async Task Success()
    {
        using var nativeClient = NativeClient.InitEcho(0, new string[] { "3000" }, 1);

        foreach (var isAsync in new bool[] { true, false })
        {
            var buffer = MemoryMarshal.Cast<Account, byte>(new Account[]
            {
                    new Account
                    {
                        Id = 1,
                        UserData128 = 2,
                        UserData64 = 3,
                        UserData32 = 4,
                        Code = 5,
                        Ledger = 6,
                        Flags = AccountFlags.Linked,
                    }
            }).ToArray();

            var callback = new CallbackSimulator<Account, UInt128>(nativeClient,
                TBOperation.LookupAccounts,
                (byte)TBOperation.LookupAccounts,
                buffer,
                PacketStatus.Ok,
                delay: 100,
                isAsync
            );

            var task = callback.Run();
            Assert.IsFalse(task.IsCompleted);

            var accounts = await task;
            Assert.IsTrue(accounts.Length == 1);
            Assert.IsTrue(accounts[0].Id == 1);
            Assert.IsTrue(accounts[0].UserData128 == 2);
            Assert.IsTrue(accounts[0].UserData64 == 3);
            Assert.IsTrue(accounts[0].UserData32 == 4);
            Assert.IsTrue(accounts[0].Code == 5);
            Assert.IsTrue(accounts[0].Ledger == 6);
            Assert.IsTrue(accounts[0].Flags == AccountFlags.Linked);
        }
    }

    private class CallbackSimulator<TResult, TBody>
        where TResult : unmanaged
        where TBody : unmanaged
    {
        private readonly Request<TResult, TBody> request;
        private readonly Packet packet;
        private readonly byte receivedOperation;
        private readonly Memory<byte> buffer;
        private readonly PacketStatus status;
        private readonly int delay;

        public CallbackSimulator(NativeClient nativeClient, TBOperation operation, byte receivedOperation, Memory<byte> buffer, PacketStatus status, int delay, bool isAsync)
        {
            unsafe
            {
                this.request = isAsync ? new AsyncRequest<TResult, TBody>(nativeClient, operation) : new BlockingRequest<TResult, TBody>(nativeClient, operation);
                this.packet = nativeClient.AcquirePacket();
                this.receivedOperation = receivedOperation;
                this.buffer = buffer;
                this.status = status;
                this.delay = delay;
            }
        }

        public Task<TResult[]> Run()
        {
            Task.Run(() =>
            {
                unsafe
                {
                    Task.Delay(delay).Wait();
                    packet.Pointer->operation = receivedOperation;
                    packet.Pointer->status = status;
                    request.Complete(packet, buffer.Span);
                }
            });

            if (request is AsyncRequest<TResult, TBody> asyncRequest)
            {
                return asyncRequest.Wait();
            }
            else if (request is BlockingRequest<TResult, TBody> blockingRequest)
            {
                return Task.Run(() => blockingRequest.Wait());
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }

}
