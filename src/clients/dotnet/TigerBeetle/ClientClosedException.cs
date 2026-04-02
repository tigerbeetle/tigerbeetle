using System;
namespace TigerBeetle;

/// <summary>
/// ClientClosedException is thrown when the client instance is closed and
/// its resources have been freed.
/// </summary>
public sealed class ClientClosedException : RequestException
{
    internal ClientClosedException() { }

    public override string Message => "Client was closed.";
}
