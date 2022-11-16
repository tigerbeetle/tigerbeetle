package com.tigerbeetle;

public final class InitializationException extends RuntimeException {

    public interface Status {
        int SUCCESS = 0;
        int UNEXPECTED = 1;
        int OUT_OF_MEMORY = 2;
        int INVALID_ADDRESS = 3;
        int SYSTEM_RESOURCES = 4;
        int NETWORK_SUBSYSTEM = 5;
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
                return "Unexpected error";

            case Status.OUT_OF_MEMORY:
                return "Out of memory";

            case Status.INVALID_ADDRESS:
                return "Invalid addresses";

            case Status.SYSTEM_RESOURCES:
                return "System Resources";

            case Status.NETWORK_SUBSYSTEM:
                return "Network subsystem";

            default:
                return "Unknown error status " + status;
        }
    }

}
