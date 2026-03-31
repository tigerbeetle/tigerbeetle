package com.tigerbeetle;

/**
 * Abstract unchecked exception that may occur during a request. See the derived exceptions for the
 * specific failures.
 **/
public abstract class RequestException extends RuntimeException {
    RequestException() {}
}
