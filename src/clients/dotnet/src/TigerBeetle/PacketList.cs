using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using static TigerBeetle.TBClient;

namespace TigerBeetle
{
	internal sealed class PacketList
	{
		#region Fields

		private readonly Client client;
		private unsafe TBPacketList packetList;

		private readonly SemaphoreSlim maxConcurrencySemaphore;

		#endregion Fields

		#region Constructor

		public unsafe PacketList(Client client, TBPacketList packetList, int maxConcurrency)
		{
			this.client = client;
			this.packetList = packetList;

			maxConcurrencySemaphore = new(maxConcurrency, maxConcurrency);
		}

		#endregion Constructor

		#region Properties

		public Client Client => client;

		#endregion Properties

		#region Methods

		public Packet Rent()
		{
			maxConcurrencySemaphore.Wait();
			unsafe
			{
				var packet = RemoveFromHead();
				return new Packet(packet);
			}
		}

		public async ValueTask<Packet> RentAsync()
		{
			await maxConcurrencySemaphore.WaitAsync();
			unsafe
			{
				var packet = RemoveFromHead();
				return new Packet(packet);
			}
		}

		private unsafe TBPacket* RemoveFromHead()
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

				var clientHandle = Client.Handle;
				TBClient.tb_client_submit(clientHandle, &packetList);
			}
		}

		public void Return(Packet packet)
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

			maxConcurrencySemaphore.Release(1);
		}

		#endregion Methods
	}
}
