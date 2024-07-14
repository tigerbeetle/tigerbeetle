using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using static TigerBeetle.AssertionException;
using static TigerBeetle.TBClient;

namespace TigerBeetle;

internal sealed class NativeClient : IDisposable
{
    private volatile IntPtr client;

    private unsafe delegate InitializationStatus InitFunction(
                IntPtr* out_client,
                UInt128Extensions.UnsafeU128 cluster_id,
                byte* address_ptr,
                uint address_len,
                IntPtr on_completion_ctx,
                delegate* unmanaged[Cdecl]<IntPtr, IntPtr, TBPacket*, byte*, uint, void> on_completion_fn
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

    public static NativeClient Init(UInt128 clusterID, string[] addresses)
    {
        unsafe
        {
            return CallInit(tb_client_init, clusterID, addresses);
        }
    }

    public static NativeClient InitEcho(UInt128 clusterID, string[] addresses)
    {
        unsafe
        {
            return CallInit(tb_client_init_echo, clusterID, addresses);
        }
    }

    private static NativeClient CallInit(InitFunction initFunction, UInt128Extensions.UnsafeU128 clusterID, string[] addresses)
    {
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
                    IntPtr.Zero,
                    &OnCompletionCallback
                );

                if (status != InitializationStatus.Success) throw new InitializationException(status);
                return new NativeClient(handle);
            }
        }
    }

    public TResult[] CallRequest<TResult, TBody>(TBOperation operation, ReadOnlySpan<TBody> batch)
        where TResult : unmanaged
        where TBody : unmanaged
    {
        unsafe
        {
            fixed (void* pointer = batch)
            {
                var blockingRequest = new BlockingRequest<TResult, TBody>(this, operation);
                blockingRequest.Submit(pointer, batch.Length);
                return blockingRequest.Wait();
            }
        }
    }

    public async Task<TResult[]> CallRequestAsync<TResult, TBody>(TBOperation operation, ReadOnlyMemory<TBody> batch)
        where TResult : unmanaged
        where TBody : unmanaged
    {
        using (var memoryHandler = batch.Pin())
        {
            var asyncRequest = new AsyncRequest<TResult, TBody>(this, operation);

            unsafe
            {
                asyncRequest.Submit(memoryHandler.Pointer, batch.Length);
            }

            return await asyncRequest.Wait().ConfigureAwait(continueOnCapturedContext: false);
        }
    }

    public void ReleasePacket(Packet packet)
    {
        unsafe
        {
            Marshal.FreeCoTaskMem((IntPtr)packet.Pointer);
        }
    }

    public void Submit(Packet packet)
    {
        unsafe
        {
            if (client != IntPtr.Zero)
            {
                lock (this)
                {
                    if (client != IntPtr.Zero)
                    {
                        tb_client_submit(client, packet.Pointer);
                        return;
                    }
                }
            }

            packet.Pointer->status = PacketStatus.ClientShutdown;
            OnComplete(packet.Pointer, null, 0);
        }
    }

    public Packet AcquirePacket()
    {
        unsafe
        {
            return new Packet((TBPacket*)Marshal.AllocCoTaskMem(sizeof(TBPacket)));
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

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private unsafe static void OnCompletionCallback(IntPtr ctx, IntPtr client, TBPacket* packet, byte* result, uint result_len)
        => OnComplete(packet, result, result_len);

    private unsafe static void OnComplete(TBPacket* packet, byte* result, uint result_len)
    {
        var request = IRequest.FromUserData(packet->userData);
        if (request != null)
        {
            var span = result_len > 0 ? new ReadOnlySpan<byte>(result, (int)result_len) : ReadOnlySpan<byte>.Empty;
            request.Complete(new Packet(packet), span);
        }
    }
}
