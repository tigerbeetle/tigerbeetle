using System;

namespace TigerBeetle
{
    public sealed class MaxConcurrencyExceededException : Exception
    {
        internal MaxConcurrencyExceededException()
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
}
