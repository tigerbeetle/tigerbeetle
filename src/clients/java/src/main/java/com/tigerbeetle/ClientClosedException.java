package com.tigerbeetle;

/**
 * ClientClosedException is thrown when the client instance is closed and its resources have been
 * freed.
 **/
public final class ClientClosedException extends RequestException {

    public ClientClosedException() {}

    @Override
    public String getMessage() {
        return toString();
    }

    @Override
    public String toString() {
        return "Client was closed.";
    }
}
