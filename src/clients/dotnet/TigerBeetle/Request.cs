using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using static TigerBeetle.AssertionException;

namespace TigerBeetle;


internal abstract class NativeRequest
{
    private GCHandle? packetHandle = null;

    protected unsafe void Submit(NativeClient nativeClient, TBOperation operation, void* data, int len)
    {
        // Create a handle to the request to ensure it's not GC'd while the packet is submitted.
        var requestHandle = GCHandle.Alloc(this, GCHandleType.Normal);

        // Create a handle to the packet itself to keep it pinned in memory for the C client.
        AssertTrue(packetHandle == null, "Submitting from an already pending NativeRequest");
        packetHandle = GCHandle.Alloc(new TBPacket
        {
            userData = GCHandle.ToIntPtr(requestHandle),
            userTag = 0,
            operation = (byte)operation,
            data = (IntPtr)data,
            dataSize = (uint)len,
        }, GCHandleType.Pinned);

        try
        {
            nativeClient.Submit((TBPacket*)packetHandle.Value.AddrOfPinnedObject());
        }
        catch (ObjectDisposedException)
        {
            packetHandle.Value.Free();
            throw;
        }
    }

    public static unsafe void OnComplete(TBPacket* packet, ReadOnlySpan<byte> result)
    {
        // Extract info from the packet before freeing it.
        var status = packet->status;
        var operation = packet->operation;
        var requestHandle = GCHandle.FromIntPtr(packet->userData);

        // Extract the request from the requestHandle.
        AssertTrue(requestHandle.IsAllocated && requestHandle.Target != null, "Invalid GCHandle given to NativeRequest.Complete packet");
        var request = (NativeRequest)requestHandle.Target!;
        requestHandle.Free();

        // Free the packet.
        AssertTrue(request.packetHandle != null, "NativeRequest completed without a valid packet");
        AssertTrue((IntPtr)packet == request.packetHandle!.Value.AddrOfPinnedObject(), "Mismatching packet tied to a NativeRequest");
        request.packetHandle.Value.Free();
        request.packetHandle = null;

        request.Complete(status, operation, result);
    }

    public abstract void Complete(PacketStatus status, byte operation, ReadOnlySpan<byte> result);
}

internal abstract class Request<TResult, TBody> : NativeRequest
    where TResult : unmanaged
    where TBody : unmanaged
{
    private readonly TBOperation operation;

    public Request(TBOperation operation) : base()
    {
        this.operation = operation;
    }

    public unsafe void Submit(NativeClient nativeClient, void* body, int bodyCount)
    {
        this.Submit(nativeClient, this.operation, body, bodyCount * sizeof(TBody));
    }

    public override void Complete(PacketStatus status, byte operation, ReadOnlySpan<byte> result)
    {
        TResult[]? array = null;
        Exception? exception = null;

        try
        {
            switch (status)
            {
                case PacketStatus.Ok:
                    unsafe
                    {
                        AssertTrue(
                            (byte)this.operation == operation,
                            "Unexpected callback operation: expected={0}, actual={1}",
                            (byte)this.operation,
                            operation
                        );

                        AssertTrue(result.Length % sizeof(TResult) == 0,
                            "Invalid received data: result.Length={0}, SizeOf({1})={2}",
                            result.Length,
                            typeof(TResult).Name,
                            sizeof(TResult)
                        );

                        array = new TResult[result.Length / sizeof(TResult)];
                        MemoryMarshal.Cast<byte, TResult>(result).CopyTo(array);
                        break;
                    }

                case PacketStatus.ClientShutdown:
                    throw new ObjectDisposedException("Client shutdown.");

                default:
                    throw new RequestException(status);
            }
        }
        catch (Exception any)
        {
            exception = any;
        }

        if (exception != null)
        {
            SetException(exception!);
        }
        else
        {
            SetResult(array!);
        }
    }

    protected abstract void SetResult(TResult[] result);

    protected abstract void SetException(Exception exception);
}

internal sealed class AsyncRequest<TResult, TBody> : Request<TResult, TBody>
    where TResult : unmanaged
    where TBody : unmanaged
{
    private readonly TaskCompletionSource<TResult[]> completionSource;

    public AsyncRequest(TBOperation operation) : base(operation)
    {
        // Hints the TPL to execute the continuation on its own thread pool thread, instead of the unamaged's callback thread:
        this.completionSource = new TaskCompletionSource<TResult[]>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public Task<TResult[]> Wait() => completionSource.Task;

    protected override void SetResult(TResult[] result) => completionSource.SetResult(result);

    protected override void SetException(Exception exception) => completionSource.SetException(exception);

}

internal sealed class BlockingRequest<TResult, TBody> : Request<TResult, TBody>
    where TResult : unmanaged
    where TBody : unmanaged
{
    private volatile TResult[]? result = null;
    private volatile Exception? exception = null;

    private bool Completed => result != null || exception != null;

    public BlockingRequest(TBOperation operation) : base(operation)
    {
    }

    public TResult[] Wait()
    {
        if (!Completed)
        {
            lock (this)
            {
                if (!Completed)
                {
                    _ = Monitor.Wait(this);
                }
            }
        }

        return result ?? throw exception!;
    }

    protected override void SetResult(TResult[] result)
    {
        lock (this)
        {
            this.result = result;
            this.exception = null;
            Monitor.Pulse(this);
        }
    }

    protected override void SetException(Exception exception)
    {
        lock (this)
        {
            this.exception = exception;
            this.result = null;
            Monitor.Pulse(this);
        }
    }
}
