package com.tigerbeetle;

/**
 * RequestException is thrown when the internal invariants of the underlying native library are
 * violated, which is expected to never happen. If tis exception is thrown, then either there is a
 * programming error in the tigerbeetle-java library itself, or there is a version mismatch between
 * the java code and the underlying native library.
 */
public final class RequestException extends RuntimeException {

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
            return "Invalid operation.";
        else if (status == PacketStatus.InvalidDataSize.value)
            return "Invalid data size.";
        else
            return "Unknown error status " + status;
    }

}
