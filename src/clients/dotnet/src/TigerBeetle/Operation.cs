namespace TigerBeetle
{
	internal enum Operation : byte
	{
		/// The value 0 is reserved to prevent a spurious zero from being interpreted as an operation.
		Reserved = 0,

		/// The value 1 is reserved to initialize the cluster.
		Init = 1,

		/// The value 2 is reserved to register a client session with the cluster.
		Register = 2,

		CreateAccounts = 3,
		CreateTransfers = 4,
		LookupAccounts = 5,
		LookupTransfers = 6,
	}
}

