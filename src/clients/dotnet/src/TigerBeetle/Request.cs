using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using static TigerBeetle.AssertionException;

namespace TigerBeetle
{
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

        void Complete(TBOperation operation, PacketStatus status, ReadOnlySpan<byte> result);
    }

    internal abstract class Request<TResult, TBody> : IRequest
        where TResult : unmanaged
        where TBody : unmanaged
    {
        private static readonly unsafe int RESULT_SIZE = sizeof(TResult);
        private static readonly unsafe int BODY_SIZE = sizeof(TBody);

        private readonly NativeClient nativeClient;
        private readonly Packet packet;
        private readonly GCHandle handle;
        private GCHandle bodyPinnedHandle;

        public Request(NativeClient nativeClient, Packet packet)
        {
            handle = GCHandle.Alloc(this, GCHandleType.Normal);

            this.nativeClient = nativeClient;
            this.packet = packet;
        }

        public IntPtr Pin(TBody[] body, out uint size)
        {
            AssertTrue(body.Length > 0, "Message body cannot be empty");
            AssertTrue(!bodyPinnedHandle.IsAllocated, "Request data is already pinned");
            bodyPinnedHandle = GCHandle.Alloc(body, GCHandleType.Pinned);

            size = (uint)(body.Length * BODY_SIZE);
            return bodyPinnedHandle.AddrOfPinnedObject();
        }

        public void Submit(TBOperation operation, TBody[] batch)
        {
            AssertTrue(handle.IsAllocated, "Request handle not allocated");

            unsafe
            {
                var ptr = packet.Pointer;
                ptr->next = null;
                ptr->userData = (IntPtr)handle;
                ptr->operation = (byte)operation;
                ptr->data = Pin(batch, out uint size);
                ptr->dataSize = size;
                ptr->status = PacketStatus.Ok;

                this.nativeClient.Submit(packet);
            }
        }

        public void Complete(TBOperation operation, PacketStatus status, ReadOnlySpan<byte> result)
        {
            TResult[]? array = null;
            Exception? exception = null;

            try
            {
                AssertTrue(handle.IsAllocated, "Request handle not allocated");
                handle.Free();

                AssertTrue(bodyPinnedHandle.IsAllocated, "Request body not allocated");
                bodyPinnedHandle.Free();

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

            nativeClient.Return(packet);

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
                else
                {
                    SetException(new RequestException(status));
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

        public AsyncRequest(NativeClient nativeClient, Packet packet) : base(nativeClient, packet)
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

        public BlockingRequest(NativeClient nativeClient, Packet packet) : base(nativeClient, packet)
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
                        Monitor.Wait(this);
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
}