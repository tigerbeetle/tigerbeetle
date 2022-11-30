package com.tigerbeetle;

public final class InitializationException extends RuntimeException {

    public interface Status {
        int SUCCESS = 0;
        int UNEXPECTED = 1;
        int OUT_OF_MEMORY = 2;
        int ADDRESS_INVALID = 3;
        int ADDRESS_LIMIT_EXCEEDED = 4;
        int PACKETS_COUNT_INVALID = 5;
        int SYSTEM_RESOURCES = 6;
        int NETWORK_SUBSYSTEM = 7;
    }

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
        switch (status) {

            case Status.UNEXPECTED:
                return "Unexpected internal error";

            case Status.OUT_OF_MEMORY:
                return "Internal client ran out of memory";

            case Status.ADDRESS_INVALID:
                return "Replica addresses format is invalid";

            case Status.ADDRESS_LIMIT_EXCEEDED:
                return "Replica addresses limit exceeded";

            case Status.PACKETS_COUNT_INVALID:
                return "Invalid maxConcurrency";

            case Status.SYSTEM_RESOURCES:
                return "Internal client ran out of system resources";

            case Status.NETWORK_SUBSYSTEM:
                return "Internal client had unexpected networking issues";

            default:
                return "Error status " + status;
        }
    }

}
