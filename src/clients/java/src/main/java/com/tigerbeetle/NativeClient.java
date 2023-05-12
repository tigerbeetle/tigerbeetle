package com.tigerbeetle;

import static com.tigerbeetle.AssertionError.assertTrue;

final class NativeClient {
    static {
        JNILoader.loadFromJar();
    }

    private volatile long contextHandle;

    public static NativeClient init(final int clusterID, final String addresses,
            final int maxConcurrency) {
        assertArgs(clusterID, addresses, maxConcurrency);
        final long contextHandle = clientInit(clusterID, addresses, maxConcurrency);
        return new NativeClient(contextHandle);
    }

    public static NativeClient initEcho(final int clusterID, final String addresses,
            final int maxConcurrency) {
        assertArgs(clusterID, addresses, maxConcurrency);
        final long contextHandle = clientInitEcho(clusterID, addresses, maxConcurrency);
        return new NativeClient(contextHandle);
    }

    private static void assertArgs(final int clusterID, final String addresses,
            final int maxConcurrency) {
        assertTrue(clusterID >= 0, "ClusterID must be positive");
        assertTrue(addresses != null, "Replica addresses cannot be null");
        assertTrue(maxConcurrency > 0, "Invalid maxConcurrency");
    }

    private NativeClient(final long contextHandle) {
        this.contextHandle = contextHandle;
    }

    public void submit(final Request<?> request) throws MaxConcurrencyExceededException {
        final var submitted = submit(contextHandle, request);
        if (!submitted)
            throw new MaxConcurrencyExceededException();
    }

    public void close() throws Exception {

        if (contextHandle != 0) {
            // Deinit and signalize that this client is closed by setting the handles to 0
            clientDeinit(contextHandle);
            contextHandle = 0;
        }
    }

    private static native boolean submit(long contextHandle, Request<?> request);

    private static native long clientInit(int clusterID, String addresses, int maxConcurrency);

    private static native long clientInitEcho(int clusterID, String addresses, int maxConcurrency);

    private static native void clientDeinit(long contextHandle);
}
