using System;

namespace TigerBeetle
{
    public sealed class AssertionException : Exception
    {
        internal AssertionException(string format, params object[] args) : base(string.Format(format, args)) { }

        internal AssertionException(Exception innerException, string format, params object[] args) : base(string.Format(format, args), innerException) { }

        internal static void AssertTrue(bool condition, string format, params object[] args)
        {
            if (!condition)
            {
                throw new AssertionException(format, args);
            }
        }
    }
}
