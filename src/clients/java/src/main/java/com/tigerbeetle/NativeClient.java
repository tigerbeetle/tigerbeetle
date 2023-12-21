package com.tigerbeetle;

import static com.tigerbeetle.AssertionError.assertTrue;
import java.util.concurrent.atomic.AtomicLong;

final class NativeClient {
    static {
        JNILoader.loadFromJar();
    }

    // Keeping the contextHandle and a reference counter guarded by
    // atomics in order to prevent the client from using a disposed
    // context during `close()`.
    private final AtomicLong contextHandle;
    private final AtomicLong contextHandleReferences;

    public static NativeClient init(final byte[] clusterID, final String addresses,
            final int concurrencyMax) {
        assertArgs(clusterID, addresses, concurrencyMax);
        final long contextHandle = clientInit(clusterID, addresses, concurrencyMax);
        return new NativeClient(contextHandle);
    }

    public static NativeClient initEcho(final byte[] clusterID, final String addresses,
            final int concurrencyMax) {
        assertArgs(clusterID, addresses, concurrencyMax);
        final long contextHandle = clientInitEcho(clusterID, addresses, concurrencyMax);
        return new NativeClient(contextHandle);
    }

    private static void assertArgs(final byte[] clusterID, final String addresses,
            final int concurrencyMax) {
        assertTrue(clusterID.length == 16, "ClusterID must be a UInt128");
        assertTrue(addresses != null, "Replica addresses cannot be null");
        assertTrue(concurrencyMax > 0, "Invalid concurrencyMax");
    }

    private NativeClient(final long contextHandle) {
        this.contextHandle = new AtomicLong(contextHandle);
        this.contextHandleReferences = new AtomicLong(0L);
    }

    public void submit(final Request<?> request) throws ConcurrencyExceededException {
        try {
            contextHandleReferences.incrementAndGet();
            final var handle = contextHandle.getAcquire();

            if (handle == 0L)
                throw new IllegalStateException("Client is closed");

            final var packet_acquire_status = submit(handle, request);
            if (packet_acquire_status == PacketAcquireStatus.ConcurrencyMaxExceeded.value) {
                throw new ConcurrencyExceededException();
            } else if (packet_acquire_status == PacketAcquireStatus.Shutdown.value) {
                throw new IllegalStateException("Client is closing");
            } else {
                assertTrue(packet_acquire_status == PacketAcquireStatus.Ok.value,
                        "PacketAcquireStatus=%d is not implemented", packet_acquire_status);
            }
        } finally {
            contextHandleReferences.decrementAndGet();
        }
    }

    public void close() {
        if (contextHandle.getAcquire() != 0L) {
            synchronized (this) {
                final var handle = contextHandle.getAcquire();
                if (handle != 0L) {
                    // Signalize that this client is closed by setting the handler to 0,
                    // and spin wait until all references that might be using the old handle could
                    // be released.
                    contextHandle.setRelease(0L);
                    while (contextHandleReferences.getAcquire() > 0L) {
                        // Thread::onSpinWait method to give JVM a hint that the following code is
                        // in a spin loop. This has no side-effect and only provides a hint to
                        // optimize spin loops in a processor specific manner.
                        Thread.onSpinWait();
                    }

                    // This function waits until all submited requests are completed, and no more
                    // packets can be acquired after that.
                    clientDeinit(handle);
                }
            }
        }
    }

    private static native int submit(long contextHandle, Request<?> request);

    private static native long clientInit(byte[] clusterID, String addresses, int concurrencyMax);

    private static native long clientInitEcho(byte[] clusterID, String addresses,
            int concurrencyMax);

    private static native void clientDeinit(long contextHandle);
}
