package com.tigerbeetle;

public final class InitializationException extends RuntimeException {

    private final int status;

    public InitializationException(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    @Override
    public String getMessage() {
        return toString();
    }

    @Override
    public String toString() {
        if (status == InitializationStatus.Unexpected.value)
            return "Unexpected internal error";
        else if (status == InitializationStatus.OutOfMemory.value)
            return "Internal client ran out of memory";
        else if (status == InitializationStatus.AddressInvalid.value)
            return "Replica addresses format is invalid";
        else if (status == InitializationStatus.AddressLimitExceeded.value)
            return "Replica addresses limit exceeded";
        else if (status == InitializationStatus.ConcurrencyMaxInvalid.value)
            return "Invalid concurrencyMax";
        else if (status == InitializationStatus.SystemResources.value)
            return "Internal client ran out of system resources";
        else if (status == InitializationStatus.NetworkSubsystem.value)
            return "Internal client had unexpected networking issues";
        else
            return "Error status " + status;
    }
}
