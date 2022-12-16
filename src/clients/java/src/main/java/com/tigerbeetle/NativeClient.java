package com.tigerbeetle;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import static com.tigerbeetle.AssertionError.assertTrue;

final class NativeClient {
    static {
        JNILoader.loadFromJar();
    }

    private final int maxConcurrency;
    private final Semaphore maxConcurrencySemaphore;
    private volatile long contextHandle;

    public static NativeClient init(final int clusterID, final String addresses,
            final int maxConcurrency) {
        assertArgs(clusterID, addresses, maxConcurrency);
        final long contextHandle = clientInit(clusterID, addresses, maxConcurrency);
        return new NativeClient(maxConcurrency, contextHandle);
    }

    public static NativeClient initEcho(final int clusterID, final String addresses,
            final int maxConcurrency) {
        assertArgs(clusterID, addresses, maxConcurrency);
        final long contextHandle = clientInitEcho(clusterID, addresses, maxConcurrency);
        return new NativeClient(maxConcurrency, contextHandle);
    }

    private static void assertArgs(final int clusterID, final String addresses,
            final int maxConcurrency) {
        assertTrue(clusterID >= 0, "ClusterID must be positive");
        assertTrue(addresses != null, "Replica addresses cannot be null");
        assertTrue(maxConcurrency > 0, "Invalid maxConcurrency");
    }

    private NativeClient(final int maxConcurrency, final long contextHandle) {
        this.contextHandle = contextHandle;
        this.maxConcurrency = maxConcurrency;
        this.maxConcurrencySemaphore = new Semaphore(maxConcurrency, false);
    }

    public void submit(final Request<?> request) {
        acquirePermit();
        submit(contextHandle, request);
    }

    private void acquirePermit() {

        // Assure that only the max number of concurrent requests can acquire a packet
        // It forces other threads to wait until a packet became available
        // We also assure that the clientHandle will be zeroed only after all permits
        // have been released
        final int TIMEOUT = 5;
        boolean acquired = false;
        do {

            if (contextHandle == 0)
                throw new IllegalStateException("Client is closed");

            try {
                acquired = maxConcurrencySemaphore.tryAcquire(TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException interruptedException) {

                // This exception should never exposed by the API to be handled by the user
                throw new AssertionError(interruptedException,
                        "Unexpected thread interruption on acquiring a packet.");
            }

        } while (!acquired);
    }

    public void releasePermit() {
        // Releasing the packet to be used by another thread
        maxConcurrencySemaphore.release();
    }

    public void close() throws Exception {

        if (contextHandle != 0) {

            // Acquire all permits, forcing to wait for any processing thread to release
            // Note that "acquireUninterruptibly(maxConcurrency)" atomically acquires the
            // permits
            // all at once, causing this operation to take longer.
            for (int i = 0; i < maxConcurrency; i++) {
                this.maxConcurrencySemaphore.acquireUninterruptibly();
            }

            // Deinit and signalize that this client is closed by setting the handles to 0
            clientDeinit(contextHandle);
            contextHandle = 0;
        }
    }

    private static native void submit(long contextHandle, Request<?> request);

    private static native long clientInit(int clusterID, String addresses, int maxConcurrency);

    private static native long clientInitEcho(int clusterID, String addresses, int maxConcurrency);

    private static native void clientDeinit(long contextHandle);
}
