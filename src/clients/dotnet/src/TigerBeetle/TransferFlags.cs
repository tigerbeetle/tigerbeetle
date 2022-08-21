using System;

namespace TigerBeetle
{
	[Flags]
	public enum TransferFlags : ushort
	{
		None = 0,
		Linked = 1 << 0,
		Pending = 1 << 1,
		PostPendingTransfer = 1 << 2,
		VoidPendingTransfer = 1 << 3,
	}
}
