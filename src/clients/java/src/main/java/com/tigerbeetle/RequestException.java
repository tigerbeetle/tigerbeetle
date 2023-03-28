package com.tigerbeetle;

public final class RequestException extends Exception {

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
        if (status == PacketStatus.TooMuchData.value)
            return "Too much data provided on this batch.";
        else if (status == PacketStatus.InvalidOperation.value)
            return "Invalid operation. Check if this client is compatible with the server's version.";
        else if (status == PacketStatus.InvalidDataSize.value)
            return "Invalid data size. Check if this client is compatible with the server's version.";
        else
            return "Unknown error status " + status;
    }

}
