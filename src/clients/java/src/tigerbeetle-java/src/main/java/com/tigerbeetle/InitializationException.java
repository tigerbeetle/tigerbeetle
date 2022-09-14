package com.tigerbeetle;

public final class InitializationException extends RuntimeException {

    public static final class Status {
        public static final int SUCCESS = 0;
        public static final int UNEXPECTED = 1;
        public static final int OUT_OF_MEMORY = 2;
        public static final int INVALID_ADDRESS = 3;
        public static final int SYSTEM_RESOURCES = 4;
        public static final int NETWORK_SUBSYSTEM = 5;
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
