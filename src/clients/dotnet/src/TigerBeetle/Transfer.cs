using System;
using System.Runtime.InteropServices;

namespace TigerBeetle
{
	[StructLayout(LayoutKind.Sequential, Size = SIZE)]
	public struct Transfer : IEquatable<Transfer>
	{
		#region InnerTypes

		[StructLayout(LayoutKind.Explicit, Size = SIZE)]
		private unsafe struct ReservedData
		{
			public const int SIZE = 16;

			[FieldOffset(0)]
			private fixed byte raw[16];

			public ReadOnlySpan<T> AsReadOnlySpan<T>()
			{
				fixed (void* ptr = raw)
				{
					return new ReadOnlySpan<T>(ptr, SIZE);
				}
			}

			public Span<T> AsSpan<T>()
			{
				fixed (void* ptr = raw)
				{
					return new Span<T>(ptr, SIZE);
				}
			}
		}

		#endregion

		#region Fields

		public const int SIZE = 128;

		private UInt128 id;

		private UInt128 debitAccountId;

		private UInt128 creditAccountId;

		private UInt128 userData;

		private ReservedData reserved;

		private UInt128 pendingId;

		private ulong timeout;

		private uint ledger;

		private ushort code;

		private TransferFlags flags;

		private ulong amount;

		private ulong timestamp;

		#endregion Fields

		#region Properties

		public UInt128 Id { get => id; set => id = value; }

		public UInt128 DebitAccountId { get => debitAccountId; set => debitAccountId = value; }
		
		public UInt128 CreditAccountId { get => creditAccountId; set => creditAccountId = value; }

		#region Documentation

		/// <summary>
		/// Opaque third-party identifier to link this transfer (many-to-one) to an external entity.
		/// </summary>

		#endregion Documentation

		public UInt128 UserData { get => userData; set => userData = value; }

		#region Documentation

		/// <summary>
		/// Reserved for accounting policy primitives.
		/// </summary>

		#endregion Documentation

		public ReadOnlySpan<byte> Reserved
		{
			get => reserved.AsReadOnlySpan<byte>();
			set => value.CopyTo(reserved.AsSpan<byte>());
		}

		#region Documentation

		/// <summary>
		/// If this transfer will post or void a pending transfer, the id of that pending transfer.
		/// </summary>

		#endregion Documentation
		
		public UInt128 PendingId { get => pendingId; set => pendingId = value; }

		public ulong Timeout { get => timeout; set => timeout = value; }

		#region Documentation

		/// <summary>
		/// A chart of accounts code describing the reason for the transfer (e.g. deposit, settlement).
		/// </summary>

		#endregion Documentation
		
		public uint Ledger { get => ledger; set => ledger = value; }

		public ushort Code { get => code; set => code = value; }

		public TransferFlags Flags { get => flags; set => flags = value; }
		
		public ulong Amount { get => amount; set => amount = value; }
		
		public ulong Timestamp { get => timestamp; set => timestamp = value; }

		#endregion Properties

		#region Methods

		public bool Equals(Transfer other)
		{
			return AsReadOnlySpan().SequenceEqual(other.AsReadOnlySpan());
		}

		public override bool Equals(object? obj)
		{
			return obj is Transfer transfer && Equals(transfer);
		}

		public override int GetHashCode()
		{
			return id.GetHashCode();
		}

		public ReadOnlySpan<byte> AsReadOnlySpan()
		{
			unsafe
			{
				fixed (void* ptr = &this)
				{
					return new ReadOnlySpan<byte>(ptr, Transfer.SIZE);
				}
			}
		}

		#endregion Methods
	}
}
