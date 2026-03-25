using System;
namespace TigerBeetle;

/// <summary>
/// ClientReleaseException is thrown when the TigerBeetle client release
/// version is incompatible with the TigerBeetle cluster release.
/// See the <see cref="ClientReleaseException.reason"/> field to check whether
/// the client is too new or too old to connect to the cluster.
/// <see href="https://docs.tigerbeetle.com/operating/upgrading"/>
/// </summary>
public sealed class ClientReleaseException : Exception
{
    public enum Reason
    {
        ClientReleaseTooLow,
        ClientReleaseTooHigh
    }

    public readonly Reason reason;

    internal ClientReleaseException(Reason reason)
    {
        this.reason = reason;
    }

    public override string Message
    {
        get
        {
            switch (reason)
            {
                case Reason.ClientReleaseTooLow: return "Client was evicted: release too old.";
                case Reason.ClientReleaseTooHigh: return "Client was evicted: release too new.";
                default: return reason.ToString();
            }
        }
    }
}
