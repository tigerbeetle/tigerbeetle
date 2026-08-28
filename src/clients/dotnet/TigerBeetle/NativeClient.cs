using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using static TigerBeetle.AssertionException;
using static TigerBeetle.Native;
using UnsafeU128 = TigerBeetle.UInt128Extensions.UnsafeU128;

namespace TigerBeetle;

internal sealed class NativeClient : IDisposable
{
    /// <summary>
    /// Pinned single-element array, created with `GC.AllocateUninitializedArray`.
    /// </summary>
    private readonly TBClient[] tb_client;

    private NativeClient(TBClient[] tb_client)
    {
        AssertTrue(tb_client.Length == 1);
        this.tb_client = tb_client;
    }

    public static NativeClient Init(UInt128 clusterID, string[] addresses)
    {
        if (addresses == null) throw new ArgumentNullException(nameof(addresses));
        var addressesBytes = Encoding.UTF8.GetBytes(string.Join(',', addresses) + "\0");
        unsafe
        {
            // Creating a pinned, single-item array to hold the client handle.
            // Although pinned, this memory will still be freed by the GC when
            // no longer referenced.
            var tb_client = GC.AllocateUninitializedArray<TBClient>(1, pinned: true);
            fixed (TBClient* client = &tb_client[0])
            fixed (byte* addressPtr = addressesBytes)
            {
                var status = tb_client_init(
                    client,
                    (UnsafeU128*)&clusterID,
                    addressPtr,
                    (uint)addressesBytes.Length - 1,
                    IntPtr.Zero,
                    &OnCompletionCallback
                );

                if (status != InitializationStatus.Success)
                {
                    throw new InitializationException(status);
                }

                return new NativeClient(tb_client);
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
            fixed (TBClient* client = &tb_client[0])
            {
                var status = tb_client_submit(client, packet);
                if (status != ClientStatus.Ok) throw new ClientClosedException();
            }
        }
    }

    public void Dispose()
    {
        unsafe
        {
            fixed (TBClient* client = &tb_client[0])
            {
                _ = tb_client_deinit(client);
            }
        }
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private unsafe static void OnCompletionCallback(IntPtr ctx, TBPacket* packet, ulong timestamp, byte* result, uint resultLen)
    {
        _ = timestamp;

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
