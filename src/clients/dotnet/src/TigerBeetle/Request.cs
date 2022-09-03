using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using static TigerBeetle.TBClient;

namespace TigerBeetle
{
	internal interface IRequest
	{
		public static IRequest? FromUserData(IntPtr userData)
		{
			var handle = GCHandle.FromIntPtr(userData);
			return handle.IsAllocated ? handle.Target as IRequest : null;
		}

		void Complete(Operation operation, TBPacketStatus status, ReadOnlySpan<byte> result);
	}

	internal abstract class Request<TResult, TBody> : IRequest
		where TResult : unmanaged
		where TBody : unmanaged
	{
		#region Fields

		private static readonly unsafe int RESULT_SIZE = sizeof(TResult);
		private static readonly unsafe int BODY_SIZE = sizeof(TBody);

		private readonly PacketList packetList;
		private readonly Packet packet;
		private readonly GCHandle handle;
		private GCHandle bodyPinnedHandle;

		#endregion Fields

		#region Constructor

		public Request(PacketList packetList, Packet packet)
		{
			handle = GCHandle.Alloc(this, GCHandleType.Normal);

			this.packetList = packetList;
			this.packet = packet;
		}

		#endregion Constructor

		#region Methods

		public IntPtr Pin(TBody[] body, out uint size)
		{
			if (bodyPinnedHandle.IsAllocated) throw new InvalidOperationException();

			bodyPinnedHandle = GCHandle.Alloc(body, GCHandleType.Pinned);
			size = (uint)(body.Length * BODY_SIZE);

			return bodyPinnedHandle.AddrOfPinnedObject();
		}

		public void Submit(Operation operation, TBody[] batch)
		{
			unsafe
			{
				var data = packet.Data;
				data->next = null;
				data->user_data = (IntPtr)handle;
				data->operation = operation;
				data->data = Pin(batch, out uint size);
				data->data_size = size;
				data->status = TBPacketStatus.Ok;

				this.packetList.Submit(packet);
			}
		}

		public void Complete(Operation operation, TBPacketStatus status, ReadOnlySpan<byte> result)
		{
			handle.Free();
			if (bodyPinnedHandle.IsAllocated) bodyPinnedHandle.Free();

			TResult[] array;

			if (status == TBPacketStatus.Ok && result.Length > 0)
			{
				array = new TResult[result.Length / RESULT_SIZE];

				var span = MemoryMarshal.Cast<byte, TResult>(result);
				span.CopyTo(array);
			}
			else
			{
				array = Array.Empty<TResult>();
			}

			packetList.Return(packet);

			if (status == TBPacketStatus.Ok)
			{
				SetResult(array);
			}
			else
			{
				var exception = new Exception($"Result={status}");
				SetException(exception);
			}
		}

		protected abstract void SetResult(TResult[] result);

		protected abstract void SetException(Exception exception);

		#endregion Methods
	}

	internal sealed class AsyncRequest<TResult, TBody> : Request<TResult, TBody>, IRequest
		where TResult : unmanaged
		where TBody : unmanaged
	{
		#region Fields

		private readonly TaskCompletionSource<TResult[]> completionSource;

		#endregion Fields

		#region Constructor

		public AsyncRequest(PacketList packetList, Packet packet) : base(packetList, packet)
		{
			#region Comments

			// Hints the TPL to execute the continuation on its own thread pool thread, instead of the unamaged's callback thread

			#endregion Comments

			this.completionSource = new TaskCompletionSource<TResult[]>(TaskCreationOptions.RunContinuationsAsynchronously);
		}

		#endregion Constructor

		#region Methods

		public Task<TResult[]> Wait() => completionSource.Task;

		protected override void SetResult(TResult[] result) => completionSource.SetResult(result);

		protected override void SetException(Exception exception) => completionSource.SetException(exception);

		#endregion Methods
	}

	internal sealed class BlockingRequest<TResult, TBody> : Request<TResult, TBody>, IRequest
		where TResult : unmanaged
		where TBody : unmanaged
	{
		#region Fields

		private TResult[]? result = null;
		private Exception? exception;

		#endregion Fields

		#region Constructor

		public BlockingRequest(PacketList packetList, Packet packet) : base(packetList, packet)
		{
		}

		#endregion Constructor

		#region Methods

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

		#endregion Methods
	}
}