using System;

namespace TigerBeetle;

public sealed class ConcurrencyExceededException : Exception
{
    internal ConcurrencyExceededException()
    {
    }

    public override string Message
    {
        get
        {
            return "The maximum configured concurrency for the client has been exceeded.";
        }
    }
}
