using System;

namespace TigerBeetle;

public sealed class RequestException : Exception
{
    public PacketStatus Status { get; }

    internal RequestException(PacketStatus status)
    {
        Status = status;
    }

    public override string Message
    {
        get
        {
            switch (Status)
            {
                case PacketStatus.TooMuchData: return "Too much data provided on this batch.";
                case PacketStatus.InvalidOperation: return "Invalid operation.";
                case PacketStatus.InvalidDataSize: return "Invalid data size.";
                case PacketStatus.ClientEvicted: return "Client was evicted.";
                case PacketStatus.ClientReleaseTooLow: return "Client was evicted: release too old.";
                case PacketStatus.ClientReleaseTooHigh: return "Client was evicted: release too new.";
                case PacketStatus.ClientShutdown: return "Client was closed.";
                default: return "Unknown error status " + Status;
            }
        }
    }
}
