package com.tigerbeetle;

public final class RequestException extends Throwable {

    private final byte status;

    public RequestException(byte status)
    {
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

            case Request.Status.TOO_MUCH_DATA:
                return "Too much data provided on this batch.";

            case Request.Status.INVALID_OPERATION:
                return "Invalid operation. Check if this client is compatible with the server's version.";

            case Request.Status.INVALID_DATA_SIZE:
                return "Invalid data size. Check if this client is compatible with the server's version.";

            default:
                return "Unknown error status " + status;
        }
    }

    
    
    

}