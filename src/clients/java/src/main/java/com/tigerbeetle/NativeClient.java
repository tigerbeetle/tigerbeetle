package com.tigerbeetle;

import static com.tigerbeetle.AssertionError.assertTrue;

final class NativeClient {
    static {
        JNILoader.loadFromJar();
    }

    private volatile long contextHandle;

    public static NativeClient init(final int clusterID, final String addresses,
            final int concurrencyMax) {
        assertArgs(clusterID, addresses, concurrencyMax);
        final long contextHandle = clientInit(clusterID, addresses, concurrencyMax);
        return new NativeClient(contextHandle);
    }

    public static NativeClient initEcho(final int clusterID, final String addresses,
            final int concurrencyMax) {
        assertArgs(clusterID, addresses, concurrencyMax);
        final long contextHandle = clientInitEcho(clusterID, addresses, concurrencyMax);
        return new NativeClient(contextHandle);
    }

    private static void assertArgs(final int clusterID, final String addresses,
            final int concurrencyMax) {
        assertTrue(clusterID >= 0, "ClusterID must be positive");
        assertTrue(addresses != null, "Replica addresses cannot be null");
        assertTrue(concurrencyMax > 0, "Invalid concurrencyMax");
    }

    private NativeClient(final long contextHandle) {
        this.contextHandle = contextHandle;
    }

    public void submit(final Request<?> request) throws ConcurrencyExceededException {
        final var packet_acquire_status = submit(contextHandle, request);
        if (packet_acquire_status == PacketAcquireStatus.ConcurrencyMaxExceeded.value) {
            throw new ConcurrencyExceededException();
        } else if (packet_acquire_status == PacketAcquireStatus.Shutdown.value) {
            throw new IllegalStateException("Client is closed");
        } else {
            assertTrue(packet_acquire_status == PacketAcquireStatus.Ok.value,
                    "PacketAcquireStatus=%d is not implemented", packet_acquire_status);
        }
    }

    public void close() throws Exception {

        if (contextHandle != 0) {
            synchronized (this) {
                if (contextHandle != 0) {
                    // Deinit and signalize that this client is closed by setting the handles to 0.
                    clientDeinit(contextHandle);
                    contextHandle = 0;
                }
            }
        }
    }

    private static native int submit(long contextHandle, Request<?> request);

    private static native long clientInit(int clusterID, String addresses, int concurrencyMax);

    private static native long clientInitEcho(int clusterID, String addresses, int concurrencyMax);

    private static native void clientDeinit(long contextHandle);
}
