package com.tigerbeetle;

import static com.tigerbeetle.AssertionError.assertTrue;

import java.lang.ref.Cleaner;

final class NativeClient implements AutoCloseable {
    private final static Cleaner cleaner;

    /*
     * Holds the `handle` in an object instance detached from `NativeClient` to provide state for
     * the cleaner to dispose native memory when the `Client` instance is GCed. Also implements
     * `Runnable` to be usable as the cleaner action.
     * https://docs.oracle.com/javase%2F9%2Fdocs%2Fapi%2F%2F/java/lang/ref/Cleaner.html
     *
     * Methods are synchronized to ensure tb_client functions aren't called on an invalid handle.
     * Safe to synchronize on NativeHandle object as it's private to NativeClient and can't be
     * arbitrarily/externally locked by the library user.
     */
    private static final class NativeHandle implements Runnable {
        private long handle;

        public NativeHandle(long handle) {
            assert handle != 0;
            this.handle = handle;
        }

        public synchronized void submit(final Request<?> request) {
            if (handle == 0) {
                throw new IllegalStateException("Client is closed");
            }

            NativeClient.submit(handle, request);
        }

        public synchronized void close() {
            if (handle == 0) {
                return;
            }

            clientDeinit(handle);
            handle = 0;
        }

        @Override
        public void run() {
            close();
        }
    }

    static {
        JNILoader.loadFromJar();
        cleaner = Cleaner.create();
    }

    private final NativeHandle handle;
    private final Cleaner.Cleanable cleanable;

    public static NativeClient init(final byte[] clusterID, final String addresses) {
        assertArgs(clusterID, addresses);
        final long contextHandle = clientInit(clusterID, addresses);
        return new NativeClient(contextHandle);
    }

    public static NativeClient initEcho(final byte[] clusterID, final String addresses) {
        assertArgs(clusterID, addresses);
        final long contextHandle = clientInitEcho(clusterID, addresses);
        return new NativeClient(contextHandle);
    }

    private static void assertArgs(final byte[] clusterID, final String addresses) {
        assertTrue(clusterID.length == 16, "ClusterID must be a UInt128");
        assertTrue(addresses != null, "Replica addresses cannot be null");
    }

    private NativeClient(final long contextHandle) {
        try {
            this.handle = new NativeHandle(contextHandle);
            this.cleanable = cleaner.register(this, handle);
        } catch (Throwable forward) {
            clientDeinit(contextHandle);
            throw forward;
        }
    }

    public void submit(final Request<?> request) {
        this.handle.submit(request);
    }

    @Override
    public void close() {
        // When the user calls `close()` or the client is used in a `try-resource` block,
        // we call `NativeHandle.close` to force it to run synchronously in the same thread.
        // Otherwise, if the user never disposes the client and `close` is never called,
        // the cleaner calls `NativeHandle.close` in another thread when the client is GCed.
        this.handle.close();

        // Unregistering the cleanable.
        cleanable.clean();
    }

    private static native void submit(long contextHandle, Request<?> request);

    private static native long clientInit(byte[] clusterID, String addresses);

    private static native long clientInitEcho(byte[] clusterID, String addresses);

    private static native void clientDeinit(long contextHandle);
}
