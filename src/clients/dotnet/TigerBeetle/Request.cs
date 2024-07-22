using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using static TigerBeetle.AssertionException;

namespace TigerBeetle;

internal unsafe interface IRequest
{
    public static IRequest? FromUserData(IntPtr userData)
    {
        var handle = GCHandle.FromIntPtr(userData);
        return handle.IsAllocated ? handle.Target as IRequest : null;
    }

    unsafe void Complete(TBPacket* packet, ReadOnlySpan<byte> result);
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

    private GCHandle packetGCHandle;

    public Request(NativeClient nativeClient, TBOperation operation)
    {
        requestGCHandle = GCHandle.Alloc(this, GCHandleType.Normal);

        this.nativeClient = nativeClient;
        this.operation = operation;
    }

    public unsafe void Submit(void* pointer, int len)
    {
        AssertTrue(pointer != null);
        AssertTrue(len > 0);
        AssertTrue(requestGCHandle.IsAllocated, "Request GCHandle not allocated");
        AssertTrue(!packetGCHandle.IsAllocated, "Packet GCHandle already allocated");

        packetGCHandle = GCHandle.Alloc(new TBPacket
        {
            next = null,
            userData = (IntPtr)requestGCHandle,
            operation = (byte)operation,
            data = new nint(pointer),
            dataSize = (uint)(len * BODY_SIZE),
            status = PacketStatus.Ok,
        }, GCHandleType.Pinned);

        this.nativeClient.Submit((TBPacket*)packetGCHandle.AddrOfPinnedObject().ToPointer());
    }

    public unsafe void Complete(TBPacket* packet, ReadOnlySpan<byte> result)
    {
        unsafe
        {
            TResult[]? array = null;
            Exception? exception = null;
            PacketStatus? status = null;

            try
            {
                AssertTrue(packet != null, "Null callback packet pointer");
                AssertTrue(requestGCHandle.IsAllocated, "Request GCHandle not allocated");
                AssertTrue(packetGCHandle.IsAllocated, "Packet GCHandle not allocated");
                AssertTrue((byte)operation == packet->operation, "Unexpected callback operation: expected={0}, actual={1}", (byte)operation, packet->operation);

                status = packet->status;

                if (status == PacketStatus.Ok && result.Length > 0)
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
            catch (Exception any)
            {
                exception = any;
            }
            finally
            {
                if (packetGCHandle.IsAllocated) packetGCHandle.Free();
                if (requestGCHandle.IsAllocated) requestGCHandle.Free();
            }

            if (exception != null)
            {
                SetException(exception);
            }
            else
            {
                if (status == PacketStatus.Ok)
                {
                    SetResult(array!);
                }
                else if (status == PacketStatus.ClientShutdown)
                {
                    SetException(new ObjectDisposedException("Client shutdown."));
                }
                else
                {
                    SetException(new RequestException(status!.Value));
                }
            }
        }
    }

    /// <summary>
    /// This is a helper for testing only
    /// Simulates calling Submit, with synchronous completion.
    /// </summary>
    internal unsafe void TestCompletion(byte operation, PacketStatus status, ReadOnlySpan<byte> result)
    {
        AssertTrue(requestGCHandle.IsAllocated, "Request GCHandle not allocated");
        AssertTrue(!packetGCHandle.IsAllocated, "Packet GCHandle already allocated");

        packetGCHandle = GCHandle.Alloc(new TBPacket
        {
            next = null,
            userData = IntPtr.Zero,
            operation = operation,
            data = 0,
            dataSize = 0,
            status = status,
        }, GCHandleType.Pinned);
        this.Complete((TBPacket*)packetGCHandle.AddrOfPinnedObject().ToPointer(), result);
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
