package com.tigerbeetle;

/**
 * ClientReleaseException is thrown when the TigerBeetle client release version is incompatible with
 * the TigerBeetle cluster release. See the
 * {@link com.tigerbeetle.ClientReleaseException#getReason()} property to check whether the client
 * is too new or too old to connect to the cluster.
 *
 * @see <a href="https://docs.tigerbeetle.com/operating/upgrading/">upgrading</a>
 */
public final class ClientReleaseException extends RequestException {

    public enum Reason {
        ClientReleaseTooLow,
        ClientReleaseTooHigh
    }

    private final Reason reason;

    ClientReleaseException(Reason reason) {
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }

    @Override
    public String getMessage() {
        return toString();
    }

    @Override
    public String toString() {
        switch (reason) {
            case ClientReleaseTooLow:
                return "Client was evicted: release too old";
            case ClientReleaseTooHigh:
                return "Client was evicted: release too new";
            default:
                return reason.toString();
        }
    }
}
