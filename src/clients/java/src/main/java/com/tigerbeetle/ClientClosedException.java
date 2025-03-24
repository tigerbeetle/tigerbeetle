package com.tigerbeetle;

public final class ClientClosedException extends IllegalStateException {

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
