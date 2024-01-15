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
                default: return "Unknown error status " + Status;
            }
        }
    }
}
