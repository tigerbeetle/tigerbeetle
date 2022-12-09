using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using static TigerBeetle.TBClient;
using static TigerBeetle.AssertionException;

namespace TigerBeetle
{
    internal interface IRequest
    {
        public static IRequest? FromUserData(IntPtr userData)
        {
            var handle = GCHandle.FromIntPtr(userData);
            return handle.IsAllocated ? handle.Target as IRequest : null;
        }

        void Complete(TBOperation operation, PacketStatus status, ReadOnlySpan<byte> result);
    }

    internal struct Packet
    {
        public readonly unsafe TBPacket* Data;

        public unsafe Packet(TBPacket* data)
        {
            Data = data;
        }
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

        public IntPtr Pin(TBody[] body, out int size)
        {
            AssertTrue(!bodyPinnedHandle.IsAllocated, "Request data is already pinned");
            bodyPinnedHandle = GCHandle.Alloc(body, GCHandleType.Pinned);

            size = body.Length * BODY_SIZE;
            return bodyPinnedHandle.AddrOfPinnedObject();
        }

        public void Submit(TBOperation operation, TBody[] batch)
        {
            AssertTrue(handle.IsAllocated, "Request handle not allocated");

            unsafe
            {
                var data = packet.Data;
                data->next = null;
                data->userData = (IntPtr)handle;
                data->operation = (byte)operation;
                data->data = Pin(batch, out int size);
                data->dataSize = size;
                data->status = PacketStatus.Ok;

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
        private TResult[]? result = null;
        private Exception? exception;

        public BlockingRequest(NativeClient nativeClient, Packet packet) : base(nativeClient, packet)
        {
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

        public TResult[] Wait()
        {
            lock (this)
            {
                Monitor.Wait(this);
                return result ?? throw exception!;
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