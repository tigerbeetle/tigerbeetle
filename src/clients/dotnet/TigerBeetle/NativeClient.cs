using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using static TigerBeetle.AssertionException;
using static TigerBeetle.TBClient;

namespace TigerBeetle
{
    internal sealed class NativeClient : IDisposable
    {
        private volatile IntPtr client;

        private unsafe delegate InitializationStatus InitFunction(
                    IntPtr* out_client,
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


        private NativeClient(IntPtr client)
        {
            this.client = client;
        }

        private static byte[] GetBytes(string[] addresses)
        {
            if (addresses == null) throw new ArgumentNullException(nameof(addresses));
            return Encoding.UTF8.GetBytes(string.Join(',', addresses) + "\0");
        }

        public static NativeClient Init(uint clusterID, string[] addresses, int concurrencyMax)
        {
            unsafe
            {
                return CallInit(tb_client_init, clusterID, addresses, concurrencyMax);
            }
        }

        public static NativeClient InitEcho(uint clusterID, string[] addresses, int concurrencyMax)
        {
            unsafe
            {
                return CallInit(tb_client_init_echo, clusterID, addresses, concurrencyMax);
            }
        }

        private static NativeClient CallInit(InitFunction initFunction, uint clusterID, string[] addresses, int concurrencyMax)
        {
            if (concurrencyMax <= 0) throw new ArgumentOutOfRangeException(nameof(concurrencyMax), "Concurrency must be positive");

            var addresses_byte = GetBytes(addresses);
            unsafe
            {
                fixed (byte* addressPtr = addresses_byte)
                {
                    IntPtr handle;

                    var status = initFunction(
                        &handle,
                        clusterID,
                        addressPtr,
                        (uint)addresses_byte.Length - 1,
                        (uint)concurrencyMax,
                        IntPtr.Zero,
#if NETSTANDARD
                        OnCompletionHandler
#else
                        &OnCompletionCallback
#endif
                    );

                    if (status != InitializationStatus.Success) throw new InitializationException(status);
                    return new NativeClient(handle);
                }
            }
        }

        public TResult[] CallRequest<TResult, TBody>(TBOperation operation, TBody[] batch)
            where TResult : unmanaged
            where TBody : unmanaged
        {
            var blockingRequest = new BlockingRequest<TResult, TBody>(this, operation);
            blockingRequest.Submit(batch);
            return blockingRequest.Wait();
        }

        public async Task<TResult[]> CallRequestAsync<TResult, TBody>(TBOperation operation, TBody[] batch)
            where TResult : unmanaged
            where TBody : unmanaged
        {
            var asyncRequest = new AsyncRequest<TResult, TBody>(this, operation);
            asyncRequest.Submit(batch);
            return await asyncRequest.Wait().ConfigureAwait(continueOnCapturedContext: false);
        }

        public void ReleasePacket(Packet packet)
        {
            unsafe
            {
                // It is unexpected for the client to be disposed here
                // Since we wait for all acquired packets to be submitted and returned before disposing.
                AssertTrue(client != IntPtr.Zero, "Client is closed");
                AssertTrue(packet.Pointer != null, "Null packet pointer");
                tb_client_release_packet(client, packet.Pointer);
            }
        }

        public void Submit(Packet packet)
        {
            unsafe
            {
                // It is unexpected for the client to be disposed here
                // Since we wait for all acquired packets to be submitted and returned before disposing.
                AssertTrue(client != IntPtr.Zero, "Client is closed");
                tb_client_submit(client, packet.Pointer);
            }
        }

        public Packet AcquirePacket()
        {
            unsafe
            {
                if (client == IntPtr.Zero) throw new ObjectDisposedException("Client is closed");

                TBPacket* packet;
                var status = tb_client_acquire_packet(client, &packet);
                switch (status)
                {
                    case PacketAcquireStatus.Ok:
                        AssertTrue(packet != null);
                        return new Packet(packet);
                    case PacketAcquireStatus.ConcurrencyMaxExceeded:
                        throw new ConcurrencyExceededException();
                    case PacketAcquireStatus.Shutdown:
                        throw new ObjectDisposedException("Client is closing");
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public void Dispose()
        {
            if (client != IntPtr.Zero)
            {
                lock (this)
                {
                    if (client != IntPtr.Zero)
                    {
                        tb_client_deinit(client);
                        this.client = IntPtr.Zero;
                    }
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