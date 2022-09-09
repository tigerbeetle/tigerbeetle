package com.tigerbeetle;

public final class RequestException extends Exception {

    public final static class Status {
        public final static byte OK = 0;
        public final static byte TOO_MUCH_DATA = 1;
        public final static byte INVALID_OPERATION = 2;
        public final static byte INVALID_DATA_SIZE = 3;
    }

    private final byte status;

    public RequestException(byte status) {
        this.status = status;
    }

    public byte getStatus() {
        return status;
    }

    @Override
    public String getMessage() {
        return toString();
    }

    @Override
    public String toString() {
        switch (status) {

            case Status.TOO_MUCH_DATA:
                return "Too much data provided on this batch.";

            case Status.INVALID_OPERATION:
                return "Invalid operation. Check if this client is compatible with the server's version.";

            case Status.INVALID_DATA_SIZE:
                return "Invalid data size. Check if this client is compatible with the server's version.";

            default:
                return "Unknown error status " + status;
        }
    }

}