using System;

namespace TigerBeetle
{
    public sealed class InitializationException : Exception
    {
        public InitializationStatus Status { get; }

        internal InitializationException(InitializationStatus status)
        {
            Status = status;
        }

        public override string Message => Status switch
        {
            InitializationStatus.Unexpected => "Unexpected internal error",
            InitializationStatus.OutOfMemory => "Internal client ran out of memory",
            InitializationStatus.AddressInvalid => "Replica addresses format is invalid",
            InitializationStatus.AddressLimitExceeded => "Replica addresses limit exceeded",
            InitializationStatus.PacketsCountInvalid => "Invalid maxConcurrency",
            InitializationStatus.SystemResources => "Internal client ran out of system resources",
            InitializationStatus.NetworkSubsystem => "Internal client had unexpected networking issues",
            _ => "Error status " + Status,
        };
    }
}
