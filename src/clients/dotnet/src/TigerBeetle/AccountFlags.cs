using System;

namespace TigerBeetle
{
	[Flags]
	public enum AccountFlags : ushort
	{
		None = 0,
		Linked = 1 << 0,
		DebitsMustNotExceedCredits = 1 << 1,
		CreditsMustNotExceedDebits = 1 << 2,
	}
}
