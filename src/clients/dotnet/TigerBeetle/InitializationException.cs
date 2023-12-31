using System;

namespace TigerBeetle;

public sealed class InitializationException : Exception
{
    public InitializationStatus Status { get; }

    internal InitializationException(InitializationStatus status)
    {
        Status = status;
    }

    public override string Message
    {
        get
        {
            switch (Status)
            {
                case InitializationStatus.Unexpected: return "Unexpected internal error";
                case InitializationStatus.OutOfMemory: return "Internal client ran out of memory";
                case InitializationStatus.AddressInvalid: return "Replica addresses format is invalid";
                case InitializationStatus.AddressLimitExceeded: return "Replica addresses limit exceeded";
                case InitializationStatus.ConcurrencyMaxInvalid: return "Invalid concurrencyMax";
                case InitializationStatus.SystemResources: return "Internal client ran out of system resources";
                case InitializationStatus.NetworkSubsystem: return "Internal client had unexpected networking issues";
                default: return "Unknown error status " + Status;
            }
        }
    }
}
