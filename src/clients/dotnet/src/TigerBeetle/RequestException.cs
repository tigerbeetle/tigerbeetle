using System;

namespace TigerBeetle
{
    public sealed class RequestException : Exception
    {
        public PacketStatus Status { get; }

        internal RequestException(PacketStatus status)
        {
            Status = status;
        }

        public override string Message => Status switch
        {
            PacketStatus.TooMuchData => "Too much data provided on this batch.",
            PacketStatus.InvalidOperation => "Invalid operation. Check if this client is compatible with the server's version.",
            PacketStatus.InvalidDataSize => "Invalid data size. Check if this client is compatible with the server's version.",
            _ => "Unknown error status " + Status,
        };
    }
}
