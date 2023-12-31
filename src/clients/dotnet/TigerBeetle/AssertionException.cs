using System;

namespace TigerBeetle;

/// <summary>
/// The exception that is thrown when an assertion used for correctness checks is triggered.
/// Correctness check assertions differ from other assertions offered by the .Net ecosystem
/// i.e. Tracer.Assert and Debug.Assert, because they are not meant to be disabled or re-routed.
/// It's recommended that the application handle AssertionException as unrecoverable fatal errors.
/// </summary>
public sealed class AssertionException : Exception
{
    internal AssertionException() { }

    internal AssertionException(string format, params object[] args) : base(string.Format(format, args)) { }

    internal static void AssertTrue(bool condition, string format, params object[] args)
    {
        if (!condition)
        {
            throw new AssertionException(format, args);
        }
    }

    internal static void AssertTrue(bool condition)
    {
        if (!condition)
        {
            throw new AssertionException();
        }
    }
}
