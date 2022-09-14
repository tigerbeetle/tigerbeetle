package com.tigerbeetle;

public final class AssertionError extends java.lang.AssertionError {
    AssertionError(String format, Object... args) {
        super(String.format(format, args));
    }

    AssertionError(Throwable cause, String format, Object... args) {
        super(String.format(format, args), cause);
    }
}
