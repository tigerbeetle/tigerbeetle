using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static TigerBeetle.AssertionException;
using static TigerBeetle.TBClient;

namespace TigerBeetle
{
    internal sealed class NativeClient : IDisposable
    {
        private volatile IntPtr client;
        private volatile IntPtr packetListHead;
        private readonly int maxConcurrency;
        private readonly SemaphoreSlim maxConcurrencySemaphore;

        private unsafe NativeClient(IntPtr client, TBPacketList packetList, int maxConcurrency)
        {
            this.client = client;
            this.packetListHead = new IntPtr(packetList.head);
            this.maxConcurrency = maxConcurrency;
            this.maxConcurrencySemaphore = new(maxConcurrency, maxConcurrency);
        }

        public static NativeClient init(uint clusterID, string addresses, int maxConcurrency)
        {
            var addresses_byte = Encoding.UTF8.GetBytes(addresses + "\0");
            unsafe
            {
                fixed (byte* addressPtr = addresses_byte)
                {
                    IntPtr handle;
                    TBPacketList packetList;

                    var status = tb_client_init(
                        &handle,
                        &packetList,
                        clusterID,
                        addressPtr,
                        (uint)addresses_byte.Length - 1,
                        (uint)maxConcurrency,
                        IntPtr.Zero,

                        // Uses either the new function pointer by value, or the old managed delegate in .Net standard
                        // https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/proposals/csharp-9.0/function-pointers
#if NETSTANDARD
					    OnCompletionHandler
#else
                        &OnCompletionCallback
#endif
                    );


                    if (status != InitializationStatus.Success) throw new InitializationException(status);
                    return new NativeClient(handle, packetList, maxConcurrency);
                }
            }
        }

        public static NativeClient initEcho(uint clusterID, string addresses, int maxConcurrency)
        {
            var addresses_byte = Encoding.UTF8.GetBytes(addresses + "\0");
            unsafe
            {
                fixed (byte* addressPtr = addresses_byte)
                {
                    IntPtr handle;
                    TBPacketList packetList;

#if NETSTANDARD
					var status = tb_client_init_echo(&handle, &packetList, clusterID, addressPtr, (uint)addresses_byte.Length - 1, (uint)maxConcurrency, IntPtr.Zero, OnCompletionHandler);
#else
                    var status = tb_client_init_echo(&handle, &packetList, clusterID, addressPtr, (uint)addresses_byte.Length - 1, (uint)maxConcurrency, IntPtr.Zero, &OnCompletionCallback);
#endif

                    if (status != InitializationStatus.Success) throw new InitializationException(status);
                    return new NativeClient(handle, packetList, maxConcurrency);
                }
            }
        }

        public TResult[] CallRequest<TResult, TBody>(TBOperation operation, TBody[] batch)
            where TResult : unmanaged
            where TBody : unmanaged
        {
            if (batch.Length == 0) return Array.Empty<TResult>();

            var packet = Rent();
            var blockingRequest = new BlockingRequest<TResult, TBody>(this, packet);

            blockingRequest.Submit(operation, batch);
            return blockingRequest.Wait();
        }

        public async Task<TResult[]> CallRequestAsync<TResult, TBody>(TBOperation operation, TBody[] batch)
            where TResult : unmanaged
            where TBody : unmanaged
        {
            if (batch.Length == 0) return Array.Empty<TResult>();

            var packet = await RentAsync();
            var asyncRequest = new AsyncRequest<TResult, TBody>(this, packet);

            asyncRequest.Submit(operation, batch);
            return await asyncRequest.Wait().ConfigureAwait(continueOnCapturedContext: false);
        }

        public void Return(Packet packet)
        {
            unsafe
            {
                ReleasePacket(packet.Data);
            }

            maxConcurrencySemaphore.Release();
        }

        public void Submit(Packet packet)
        {
            unsafe
            {
                // It is unexpected for the client to be disposed here
                // Since we wait for all acquired packets to be submited and returned before disposing
                AssertTrue(client != IntPtr.Zero, "Client is closed");

                var data = packet.Data;
                var packetList = new TBPacketList
                {
                    head = data,
                    tail = data,
                };

                tb_client_submit(client, &packetList);
            }
        }

        private Packet Rent()
        {
            do
            {
                // This client can be disposed
                if (client == IntPtr.Zero) throw new ObjectDisposedException(nameof(client));
            } while (!maxConcurrencySemaphore.Wait(millisecondsTimeout: 5));

            unsafe
            {
                var packet = AcquirePacket();
                return new Packet(packet);
            }
        }

        private async ValueTask<Packet> RentAsync()
        {
            do
            {
                // This client can be disposed
                if (client == IntPtr.Zero) throw new ObjectDisposedException(nameof(client));
            } while (!await maxConcurrencySemaphore.WaitAsync(millisecondsTimeout: 5));

            unsafe
            {
                var packet = AcquirePacket();
                return new Packet(packet);
            }
        }

        private unsafe TBPacket* AcquirePacket()
        {
            var headPtr = packetListHead;
            while (true)
            {
                if (headPtr == IntPtr.Zero) return null;

                var head = (TBPacket*)headPtr.ToPointer();
                var nextPtr = new IntPtr(head->next);
                var currentPtr = Interlocked.CompareExchange(ref packetListHead, nextPtr, headPtr);
                if (currentPtr == headPtr)
                {
                    head->next = null;
                    return head;
                }
                else
                {
                    headPtr = currentPtr;
                }
            }
        }

        private unsafe void ReleasePacket(TBPacket* packet)
        {
            var headPtr = packetListHead;
            while (true)
            {
                packet->next = (TBPacket*)headPtr.ToPointer();
                var currentPtr = Interlocked.CompareExchange(ref packetListHead, new IntPtr(packet), headPtr);
                if (currentPtr == headPtr)
                {
                    break;
                }
                else
                {
                    headPtr = currentPtr;
                }
            }
        }

        public void Dispose()
        {
            lock (this)
            {
                if (client != IntPtr.Zero)
                {
                    for (int i = 0; i < maxConcurrency; i++)
                    {
                        maxConcurrencySemaphore.Wait();
                    }

                    tb_client_deinit(client);
                    client = IntPtr.Zero;
                }
            }
        }

        // Uses either the new function pointer by value, or the old managed delegate in .Net standard
        // Using managed delegate, the instance must be referenced to prevents GC.

#if NETSTANDARD
		private static readonly OnCompletionFn OnCompletionHandler = new OnCompletionFn(OnCompletionCallback);
		[AllowReversePInvokeCalls]
#else
        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
#endif
        private unsafe static void OnCompletionCallback(IntPtr ctx, IntPtr client, TBPacket* packet, byte* result, uint result_len)
        {
            var request = IRequest.FromUserData(packet->userData);
            if (request != null)
            {
                var span = result_len > 0 ? new ReadOnlySpan<byte>(result, (int)result_len) : ReadOnlySpan<byte>.Empty;
                request.Complete((TBOperation)packet->operation, packet->status, span);
            }
        }
    }
}