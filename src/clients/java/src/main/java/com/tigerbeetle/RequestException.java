package com.tigerbeetle;

public final class RequestException extends Exception {

    public interface Status {
        byte OK = 0;
        byte TOO_MUCH_DATA = 1;
        byte INVALID_OPERATION = 2;
        byte INVALID_DATA_SIZE = 3;
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
