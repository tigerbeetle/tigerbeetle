using System;
using System.Runtime.InteropServices;

namespace TigerBeetle
{
	[StructLayout(LayoutKind.Sequential, Size = SIZE)]
	public struct Account : IEquatable<Account>
	{
		#region InnerTypes

		[StructLayout(LayoutKind.Explicit, Size = SIZE)]
		private unsafe struct ReservedData
		{
			public const int SIZE = 48;

			[FieldOffset(0)]
			private fixed byte raw[SIZE];

			public ReadOnlySpan<T> AsReadOnlySpan<T>()
			{
				fixed (void* ptr = &this)
				{
					return new ReadOnlySpan<T>(ptr, SIZE);
				}
			}

			public Span<T> AsSpan<T>()
			{
				fixed (void* ptr = &this)
				{
					return new Span<T>(ptr, SIZE);
				}
			}
		}

		#endregion

		#region Fields

		public const int SIZE = 128;

		private UInt128 id;

		private UInt128 userData;

		private ReservedData reserved;

		private uint ledger;

		private ushort code;

		private AccountFlags flags;

		private ulong debitsPending;

		private ulong debitsPosted;

		private ulong creditsPending;

		private ulong creditsPosted;

		private ulong timestamp;

		#endregion Fields

		#region Properties

		public UInt128 Id { get => id; set => id = value; }

		#region Documentation

		/// <summary>
		/// Opaque third-party identifier to link this account (many-to-one) to an external entity.
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

		public uint Ledger { get => ledger; set => ledger = value; }

		#region Documentation

		/// <summary>
		/// A chart of accounts code describing the type of account (e.g. clearing, settlement).
		/// </summary>

		#endregion Documentation

		public ushort Code { get => code; set => code = value; }
		
		public AccountFlags Flags { get => flags; set => flags = value; }

		public ulong DebitsPending { get => debitsPending; set => debitsPending = value; }
		
		public ulong DebitsPosted { get => debitsPosted; set => debitsPosted = value; }
		
		public ulong CreditsPending { get => creditsPending; set => creditsPending = value; }
		
		public ulong CreditsPosted { get => creditsPosted; set => creditsPosted = value; }
		
		public ulong Timestamp { get => timestamp; set => timestamp = value; }

		#endregion Properties

		#region Methods

		public bool Equals(Account other)
		{
			return AsReadOnlySpan().SequenceEqual(other.AsReadOnlySpan());
		}

		public override bool Equals(object? obj)
		{
			return obj is Account account && Equals(account);
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
					return new ReadOnlySpan<byte>(ptr, Account.SIZE);
				}
			}
		}

		#endregion Methods
	}
}
