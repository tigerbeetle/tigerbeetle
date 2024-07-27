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
    // Once the client handle is set, all interactions with it are sychronized using `lock(this)`
    // to prevent threads from accidentally using a deinitialized client handle. It's safe to
    // synchronize on the NativeClient object as it's private to Client and can't be arbitrarily
    // or externally locked by the library user.
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
                var blockingRequest = new BlockingRequest<TResult, TBody>(operation);
                blockingRequest.Submit(this, pointer, batch.Length);
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
            var asyncRequest = new AsyncRequest<TResult, TBody>(operation);

            unsafe
            {
                asyncRequest.Submit(this, memoryHandler.Pointer, batch.Length);
            }

            return await asyncRequest.Wait().ConfigureAwait(continueOnCapturedContext: false);
        }
    }

    public unsafe void Submit(TBPacket* packet)
    {
        unsafe
        {
            lock (this)
            {
                if (client != IntPtr.Zero)
                {
                    tb_client_submit(client, packet);
                    return;
                }
            }

            packet->status = PacketStatus.ClientShutdown;
            OnComplete(packet, null, 0);
        }
    }

    public void Dispose()
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

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private unsafe static void OnCompletionCallback(IntPtr ctx, IntPtr client, TBPacket* packet, byte* result, uint resultLen)
    {
        try
        {
            AssertTrue(ctx == IntPtr.Zero);
            OnComplete(packet, result, resultLen);
        }
        catch (Exception e)
        {
            // The caller is unmanaged code, so if an exception occurs here we should force panic.
            Environment.FailFast("Failed to process a packet in the OnCompletionCallback", e);
        }
    }

    private unsafe static void OnComplete(TBPacket* packet, byte* result, uint resultLen)
    {
        var span = resultLen > 0 ? new ReadOnlySpan<byte>(result, (int)resultLen) : ReadOnlySpan<byte>.Empty;
        NativeRequest.OnComplete(packet, span);
    }
}
