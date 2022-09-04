using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using static TigerBeetle.TBClient;

namespace TigerBeetle
{
	internal sealed class PacketList : IDisposable
	{
		#region Fields

		private IntPtr client;
		private unsafe TBPacketList packetList;
		private readonly int maxConcurrency;
		private readonly SemaphoreSlim maxConcurrencySemaphore;

		#endregion Fields

		#region Constructor

		public unsafe PacketList(IntPtr client, TBPacketList packetList, int maxConcurrency)
		{
			this.client = client;
			this.packetList = packetList;
			this.maxConcurrency = maxConcurrency;
			this.maxConcurrencySemaphore = new(maxConcurrency, maxConcurrency);
		}

		#endregion Constructor

		#region Properties

		public IntPtr Client => client;

		#endregion Properties

		#region Methods

		public Packet Rent()
		{
			do
			{
				// This client can be disposed
				if (client == IntPtr.Zero) throw new ObjectDisposedException(nameof(Client));
			} while (!maxConcurrencySemaphore.Wait(millisecondsTimeout: 5));

			unsafe
			{
				var packet = Pop();
				return new Packet(packet);
			}
		}

		public async ValueTask<Packet> RentAsync()
		{
			do 
			{
				// This client can be disposed
				if (client == IntPtr.Zero) throw new ObjectDisposedException(nameof(Client));
			} while (!await maxConcurrencySemaphore.WaitAsync(millisecondsTimeout: 5));

			unsafe
			{
				var packet = Pop();
				return new Packet(packet);
			}
		}

		public void Return(Packet packet)
		{
			Push(packet);
			maxConcurrencySemaphore.Release();
		}

		public void Submit(Packet packet)
		{
			unsafe
			{
				var data = packet.Data;
				var packetList = new TBPacketList
				{
					head = data,
					tail = data,
				};

				// It is unexpected for the client to be disposed here
				// Since we wait for all acquired packets to be submited and returned before disposing
				Debug.Assert(client != IntPtr.Zero);

				tb_client_submit(client, &packetList);
			}
		}

		private unsafe TBPacket* Pop()
		{
			lock (this)
			{
				var packet = packetList.head;
				if (packet == null) throw new InvalidOperationException();

				if (packet == packetList.tail)
				{
					packetList.head = null;
					packetList.tail = null;
				}
				else
				{
					packetList.head = packet->next;
				}

				packet->next = null;
				return packet;
			}
		}

		private void Push(Packet packet)
		{
			lock (this)
			{
				unsafe
				{
					var tail = packet.Data;
					tail->next = null;

					if (packetList.tail == null)
					{
						packetList.head = tail;
						packetList.tail = tail;
					}
					else
					{
						packetList.tail->next = tail;
						packetList.tail = tail;
					}
				}
			}
		}

		public void Dispose()
		{
			if (client != IntPtr.Zero)
			{
				for (int i = 0; i < maxConcurrency; i++)
				{
					maxConcurrencySemaphore.Wait();
				}

				lock (this)
				{
					tb_client_deinit(client);
					client = IntPtr.Zero;
				}
			}
		}

		#endregion Methods
	}
}