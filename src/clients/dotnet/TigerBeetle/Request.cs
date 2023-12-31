using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using static TigerBeetle.AssertionException;

namespace TigerBeetle;

internal struct Packet
{
    public readonly unsafe TBPacket* Pointer;

    public unsafe Packet(TBPacket* pointer)
    {
        Pointer = pointer;
    }
}

internal interface IRequest
{
    public static IRequest? FromUserData(IntPtr userData)
    {
        var handle = GCHandle.FromIntPtr(userData);
        return handle.IsAllocated ? handle.Target as IRequest : null;
    }

    void Complete(Packet packet, ReadOnlySpan<byte> result);
}

internal abstract class Request<TResult, TBody> : IRequest
    where TResult : unmanaged
    where TBody : unmanaged
{
    private static readonly unsafe int RESULT_SIZE = sizeof(TResult);
    private static readonly unsafe int BODY_SIZE = sizeof(TBody);

    private readonly NativeClient nativeClient;
    private readonly TBOperation operation;
    private readonly GCHandle requestGCHandle;
    private GCHandle bodyGCHandle;

    public Request(NativeClient nativeClient, TBOperation operation)
    {
        requestGCHandle = GCHandle.Alloc(this, GCHandleType.Normal);

        this.nativeClient = nativeClient;
        this.operation = operation;
    }

    private IntPtr Pin(TBody[] body, out uint size)
    {
        AssertTrue(body.Length > 0, "Message body cannot be empty");
        AssertTrue(!bodyGCHandle.IsAllocated, "Request body GCHandle is already allocated");
        bodyGCHandle = GCHandle.Alloc(body, GCHandleType.Pinned);

        size = (uint)(body.Length * BODY_SIZE);
        return bodyGCHandle.AddrOfPinnedObject();
    }

    public void Submit(TBody[] batch)
    {
        if (batch == null) throw new ArgumentNullException(nameof(batch));
        if (batch.Length == 0) throw new ArgumentException("Batch cannot be empty", nameof(batch));

        AssertTrue(requestGCHandle.IsAllocated, "Request GCHandle not allocated");

        unsafe
        {
            var packet = this.nativeClient.AcquirePacket();

            var ptr = packet.Pointer;
            ptr->next = null;
            ptr->userData = (IntPtr)requestGCHandle;
            ptr->operation = (byte)operation;
            ptr->data = Pin(batch, out uint size);
            ptr->dataSize = size;
            ptr->status = PacketStatus.Ok;

            this.nativeClient.Submit(packet);
        }
    }

    public void Complete(Packet packet, ReadOnlySpan<byte> result)
    {
        unsafe
        {
            TResult[]? array = null;
            Exception? exception = null;

            try
            {
                AssertTrue(packet.Pointer != null, "Null callback packet pointer");

                try
                {
                    AssertTrue((byte)operation == packet.Pointer->operation, "Unexpected callback operation: expected={0}, actual={1}", (byte)operation, packet.Pointer->operation);

                    if (packet.Pointer->status == PacketStatus.Ok && result.Length > 0)
                    {
                        AssertTrue(result.Length % RESULT_SIZE == 0,
                            "Invalid received data: result.Length={0}, SizeOf({1})={2}",
                            result.Length,
                            typeof(TResult).Name,
                            RESULT_SIZE
                        );

                        array = new TResult[result.Length / RESULT_SIZE];

                        var span = MemoryMarshal.Cast<byte, TResult>(result);
                        span.CopyTo(array);
                    }
                    else
                    {
                        array = Array.Empty<TResult>();
                    }
                }
                finally
                {
                    nativeClient.ReleasePacket(packet);
                }

                if (requestGCHandle.IsAllocated) requestGCHandle.Free();
                if (bodyGCHandle.IsAllocated) bodyGCHandle.Free();
            }
            catch (Exception any)
            {
                exception = any;
            }

            if (exception != null)
            {
                SetException(exception);
            }
            else
            {
                if (packet.Pointer->status == PacketStatus.Ok)
                {
                    SetResult(array!);
                }
                else
                {
                    SetException(new RequestException(packet.Pointer->status));
                }
            }
        }
    }

    protected abstract void SetResult(TResult[] result);

    protected abstract void SetException(Exception exception);
}

internal sealed class AsyncRequest<TResult, TBody> : Request<TResult, TBody>, IRequest
    where TResult : unmanaged
    where TBody : unmanaged
{
    private readonly TaskCompletionSource<TResult[]> completionSource;

    public AsyncRequest(NativeClient nativeClient, TBOperation operation) : base(nativeClient, operation)
    {
        // Hints the TPL to execute the continuation on its own thread pool thread, instead of the unamaged's callback thread:
        this.completionSource = new TaskCompletionSource<TResult[]>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public Task<TResult[]> Wait() => completionSource.Task;

    protected override void SetResult(TResult[] result) => completionSource.SetResult(result);

    protected override void SetException(Exception exception) => completionSource.SetException(exception);

}

internal sealed class BlockingRequest<TResult, TBody> : Request<TResult, TBody>, IRequest
    where TResult : unmanaged
    where TBody : unmanaged
{
    private volatile TResult[]? result = null;
    private volatile Exception? exception = null;

    private bool Completed => result != null || exception != null;

    public BlockingRequest(NativeClient nativeClient, TBOperation operation) : base(nativeClient, operation)
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
