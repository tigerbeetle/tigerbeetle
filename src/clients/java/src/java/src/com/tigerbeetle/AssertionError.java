package com.tigerbeetle;

/**
 * {@code AssertionError} is a <em>unchecked exception</em>
 * that is can be thrown when any unexpected value is received by TigerBeetle.
 * This exception should be logged with full stack trace.
 */
public final class AssertionError extends Error {
    AssertionError(String format, Object... args) {
        super(String.format(format, args));
    }

    AssertionError(Throwable cause, String format, Object... args) {
        super(String.format(format, args), cause);
    }    
}
