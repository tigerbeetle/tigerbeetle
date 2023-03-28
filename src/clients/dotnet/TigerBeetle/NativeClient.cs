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

        private unsafe delegate InitializationStatus InitFunction(
                    IntPtr* out_client,
                    TBPacketList* out_packets,
                    uint cluster_id,
                    byte* address_ptr,
                    uint address_len,
                    uint num_packets,
                    IntPtr on_completion_ctx,

                    // Uses either the new function pointer by value, or the old managed delegate in .Net standard
                    // https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/proposals/csharp-9.0/function-pointers
#if NETSTANDARD
                    [MarshalAs(UnmanagedType.FunctionPtr)]
                    OnCompletionFn on_completion_fn
#else
                    delegate* unmanaged[Cdecl]<IntPtr, IntPtr, TBPacket*, byte*, uint, void> on_completion_fn
#endif
                );


        private unsafe NativeClient(IntPtr client, TBPacketList packetList, int maxConcurrency)
        {
            this.client = client;
            this.packetListHead = new IntPtr(packetList.head);
            this.maxConcurrency = maxConcurrency;
            this.maxConcurrencySemaphore = new(maxConcurrency, maxConcurrency);
        }

        private static byte[] GetBytes(string[] addresses)
        {
            if (addresses == null) throw new ArgumentNullException(nameof(addresses));
            return Encoding.UTF8.GetBytes(string.Join(',', addresses) + "\0");
        }

        public static NativeClient Init(uint clusterID, string[] addresses, int maxConcurrency)
        {
            unsafe
            {
                return CallInit(tb_client_init, clusterID, addresses, maxConcurrency);
            }
        }

        public static NativeClient InitEcho(uint clusterID, string[] addresses, int maxConcurrency)
        {
            unsafe
            {
                return CallInit(tb_client_init_echo, clusterID, addresses, maxConcurrency);
            }
        }

        private static NativeClient CallInit(InitFunction initFunction, uint clusterID, string[] addresses, int maxConcurrency)
        {
            if (maxConcurrency <= 0) throw new ArgumentException("Max concurrency must be positive", nameof(maxConcurrency));

            var addresses_byte = GetBytes(addresses);
            unsafe
            {
                fixed (byte* addressPtr = addresses_byte)
                {
                    IntPtr handle;
                    TBPacketList packetList;

                    var status = initFunction(
                        &handle,
                        &packetList,
                        clusterID,
                        addressPtr,
                        (uint)addresses_byte.Length - 1,
                        (uint)maxConcurrency,
                        IntPtr.Zero,
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

        public TResult[] CallRequest<TResult, TBody>(TBOperation operation, TBody[] batch)
            where TResult : unmanaged
            where TBody : unmanaged
        {
            var packet = Rent();
            var blockingRequest = new BlockingRequest<TResult, TBody>(this, operation);

            blockingRequest.Submit(batch, packet);
            return blockingRequest.Wait();
        }

        public async Task<TResult[]> CallRequestAsync<TResult, TBody>(TBOperation operation, TBody[] batch)
            where TResult : unmanaged
            where TBody : unmanaged
        {
            var packet = await RentAsync();
            var asyncRequest = new AsyncRequest<TResult, TBody>(this, operation);

            asyncRequest.Submit(batch, packet);
            return await asyncRequest.Wait().ConfigureAwait(continueOnCapturedContext: false);
        }

        public void Return(Packet packet)
        {
            ReleasePacket(packet);
            maxConcurrencySemaphore.Release();
        }

        public void Submit(Packet packet)
        {
            unsafe
            {
                // It is unexpected for the client to be disposed here
                // Since we wait for all acquired packets to be submitted and returned before disposing
                AssertTrue(client != IntPtr.Zero, "Client is closed");

                var packetList = new TBPacketList
                {
                    head = packet.Pointer,
                    tail = packet.Pointer,
                };

                tb_client_submit(client, &packetList);
            }
        }

        public Packet Rent()
        {
            do
            {
                // This client can be disposed
                if (client == IntPtr.Zero) throw new ObjectDisposedException(nameof(client));
            } while (!maxConcurrencySemaphore.Wait(millisecondsTimeout: 5));

            return AcquirePacket();
        }

        public async ValueTask<Packet> RentAsync()
        {
            do
            {
                // This client can be disposed
                if (client == IntPtr.Zero) throw new ObjectDisposedException(nameof(client));
            } while (!await maxConcurrencySemaphore.WaitAsync(millisecondsTimeout: 5));

            return AcquirePacket();
        }

        private Packet AcquirePacket()
        {
            unsafe
            {
                var headPtr = packetListHead;
                while (true)
                {
                    // It is unexpected to be null here,
                    // since the semaphore restricts how many threads can acquire a packet.
                    AssertTrue(headPtr != IntPtr.Zero);

                    var head = (TBPacket*)headPtr.ToPointer();
                    var nextPtr = new IntPtr(head->next);
                    var currentPtr = Interlocked.CompareExchange(ref packetListHead, nextPtr, headPtr);
                    if (currentPtr == headPtr)
                    {
                        head->next = null;
                        return new Packet(head);
                    }
                    else
                    {
                        headPtr = currentPtr;
                    }
                }
            }
        }

        private void ReleasePacket(Packet packet)
        {
            unsafe
            {
                AssertTrue(packet.Pointer != null, "Null packet pointer");

                var headPtr = packetListHead;
                while (true)
                {
                    packet.Pointer->next = (TBPacket*)headPtr.ToPointer();
                    var currentPtr = Interlocked.CompareExchange(ref packetListHead, new IntPtr(packet.Pointer), headPtr);
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
		private unsafe static readonly OnCompletionFn OnCompletionHandler = new OnCompletionFn(OnCompletionCallback);
		
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
                request.Complete(new Packet(packet), span);
            }
        }
    }
}