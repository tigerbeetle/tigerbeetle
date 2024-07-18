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
        private long handle;
        private final AtomicLong atomicHandleReferences;
        private final AtomicLong atomicHandleClosePending;

        private static final long REF_CLOSED = 1 << 0;
        private static final long REF_ACCESS = 1 << 1;

        public NativeHandle(long handle) {
            this.handle = handle;
            this.atomicHandleReferences = new AtomicLong(0);
            this.atomicHandleClosePending = new AtomicLong(0);
        }

        // Silence PMD hint on `if (x) { if (y) {} }`. It is cleaner than `if (x && y)` here.
        @SuppressWarnings("PMD.CollapsibleIfStatements")
        public void submit(final Request<?> request) throws Exception {
            // Bump refs to access. Bail if REF_CLOSED bit was set.
            // After observing REF_CLOSED, any other modifications which dont unset it don't matter.
            if ((atomicHandleReferences.addAndGet(REF_ACCESS) & REF_CLOSED) != 0) {
                throw new IllegalStateException("Client is closed");
            }

            try {
                NativeClient.submit(handle, request);
            } finally {
                // After accessing the handle, remove our access from the ref.
                // Observing REF_CLOSED bit set means close() happened during the submit()
                // so remove an access from ClosePending. First thread to make ClosePending
                // equal to zero knows no other thread is accessing the handle and can free it.
                if ((atomicHandleReferences.addAndGet(-REF_ACCESS) & REF_CLOSED) != 0) {
                    if (atomicHandleClosePending.addAndGet(-REF_ACCESS) == 0) {
                        clientDeinit(handle);
                    }
                }
            }
        }

        public void close() {
            // Observe all REF_ACCESS' while resetting them and setting the REF_CLOSED bit.
            // Seeing the REF_CLOSED bit already set implies close() has already started so bail.
            final var refs = atomicHandleReferences.getAndSet(REF_CLOSED);
            if ((refs & REF_CLOSED) != 0) {
                return;
            }

            // Add the REF_ACCESS' observed when setting REF_CLOSED to ClosePending.
            // That many threads are still accessing the handle. When they decrement and see
            // REF_CLOSED, they will decrement ClosePending to cancel out our matching addition.
            // First of either the accessing threads or us to make ClosePending 0 frees the handle.
            if (atomicHandleClosePending.addAndGet(refs) == 0) {
                clientDeinit(handle);
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
