package com.tigerbeetle;

import static com.tigerbeetle.AssertionError.assertTrue;

import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicLong;

final class NativeClient implements AutoCloseable {
    private final static Cleaner cleaner;

    /*
     * Holds the `handle` in an object instance detached from `NativeClient` to provide state for
     * the cleaner to dispose native memory when the `Client` instance is GCed. Also implements
     * `Runnable` to be usable as the cleaner action.
     * https://docs.oracle.com/javase%2F9%2Fdocs%2Fapi%2F%2F/java/lang/ref/Cleaner.html
     */
    private static final class NativeHandle implements Runnable {
        // Keeping the contextHandle and a reference counter guarded by
        // atomics in order to prevent the client from using a disposed
        // context during `close()`.
        private final AtomicLong atomicHandle;
        private final AtomicLong atomicHandleReferences;

        public NativeHandle(long handle) {
            this.atomicHandle = new AtomicLong(handle);
            this.atomicHandleReferences = new AtomicLong(0);
        }

        public void submit(final Request<?> request) throws Exception {
            try {
                atomicHandleReferences.incrementAndGet();
                final var handle = atomicHandle.getAcquire();

                if (handle == 0L)
                    throw new IllegalStateException("Client is closed");

                NativeClient.submit(handle, request);

            } finally {
                atomicHandleReferences.decrementAndGet();
            }
        }

        public void close() {
            if (atomicHandle.getAcquire() != 0L) {
                synchronized (this) {
                    final var handle = atomicHandle.getAcquire();
                    if (handle != 0L) {
                        // Signalize that this client is closed by setting the handler to 0,
                        // and spin wait until all references that might be using the old handle
                        // could be released.
                        atomicHandle.setRelease(0L);
                        while (atomicHandleReferences.getAcquire() > 0L) {
                            // Thread::onSpinWait method to give JVM a hint that the following code
                            // is in a spin loop. This has no side-effect and only provides a hint
                            // to optimize spin loops in a processor specific manner.
                            Thread.onSpinWait();
                        }

                        // This function waits until all submitted requests are completed, and no
                        // more packets can be acquired after that.
                        clientDeinit(handle);
                    }
                }
            }
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

    public void submit(final Request<?> request) throws Exception {
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
