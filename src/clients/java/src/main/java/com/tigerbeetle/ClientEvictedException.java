package com.tigerbeetle;

/**
 * ClientEvictedException is thrown when the client is evicted from the TigerBeetle cluster. If this
 * exception is thrown, then either there are too many clients connected or the client was idle for
 * too long.
 **/
public final class ClientEvictedException extends RequestException {

    ClientEvictedException() {}

    @Override
    public String getMessage() {
        return toString();
    }

    @Override
    public String toString() {
        return "Client was evicted.";
    }

}
