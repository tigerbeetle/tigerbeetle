using System;
namespace TigerBeetle;

/// <summary>
/// TooMuchDataException is thrown when the number of events or expected results
/// exceeds the maximum message size.
/// If this exception is thrown, then either there are too many elements in a batch,
/// or the limit of a query is too large to be fulfilled in a single request.
/// </summary>
public sealed class TooMuchDataException : Exception
{
    internal TooMuchDataException() { }
    public override string Message => "Too much data was sent or requested in this batch.";
}
