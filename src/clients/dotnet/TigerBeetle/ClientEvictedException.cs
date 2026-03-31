using System;
namespace TigerBeetle;

/// <summary>
/// ClientEvictedException is thrown when the client is evicted from
/// the TigerBeetle cluster.
/// If this exception is thrown, then either there are too many clients
/// connected or the client was idle for too long.
/// </summary>
public sealed class ClientEvictedException : RequestException
{
    internal ClientEvictedException() { }

    public override string Message => "Client was evicted.";
}
