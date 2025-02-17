package com.tigerbeetle;

import static com.tigerbeetle.AssertionError.assertTrue;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;

final class NativeClient implements AutoCloseable {
    private final static Cleaner cleaner;

    /*
     * Holds the `tb_client` buffer in an object instance detached from `NativeClient` to provide
     * state for the cleaner to dispose native memory when the `Client` instance is GCed. Also
     * implements `Runnable` to be usable as the cleaner action.
     * https://docs.oracle.com/javase%2F9%2Fdocs%2Fapi%2F%2F/java/lang/ref/Cleaner.html
     */
    private static final class CleanableState implements Runnable {
        private ByteBuffer tb_client;

        public CleanableState(ByteBuffer tb_client) {
            assertTrue(tb_client.isDirect(), "Invalid client buffer");
            this.tb_client = tb_client;
        }

        public void submit(final Request<?> request) {
            NativeClient.submit(tb_client, request);
        }

        public void close() {
            clientDeinit(tb_client);
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

    private final CleanableState state;
    private final Cleaner.Cleanable cleanable;

    public static NativeClient init(final byte[] clusterID, final String addresses) {
        assertArgs(clusterID, addresses);
        final var tb_client = ByteBuffer.allocateDirect(TBClient.SIZE + TBClient.ALIGNMENT);
        clientInit(tb_client, clusterID, addresses);
        return new NativeClient(tb_client);
    }

    public static NativeClient initEcho(final byte[] clusterID, final String addresses) {
        assertArgs(clusterID, addresses);
        final var tb_client = ByteBuffer.allocateDirect(TBClient.SIZE + TBClient.ALIGNMENT);
        clientInitEcho(tb_client, clusterID, addresses);
        return new NativeClient(tb_client);
    }

    private static void assertArgs(final byte[] clusterID, final String addresses) {
        assertTrue(clusterID.length == 16, "ClusterID must be a UInt128");
        assertTrue(addresses != null, "Replica addresses cannot be null");
    }

    private NativeClient(final ByteBuffer tb_client) {
        try {
            this.state = new CleanableState(tb_client);
            this.cleanable = cleaner.register(this, state);
        } catch (Throwable forward) {
            clientDeinit(tb_client);
            throw forward;
        }
    }

    public void submit(final Request<?> request) {
        this.state.submit(request);
    }

    @Override
    public void close() {
        // When the user calls `close()` or the client is used in a `try-resource` block,
        // we call `NativeHandle.close` to force it to run synchronously in the same thread.
        // Otherwise, if the user never disposes the client and `close` is never called,
        // the cleaner calls `NativeHandle.close` in another thread when the client is GCed.
        this.state.close();

        // Unregistering the cleanable.
        cleanable.clean();
    }

    private static native void submit(ByteBuffer tb_client, Request<?> request)
            throws ClientClosedException;

    private static native void clientInit(ByteBuffer tb_client, byte[] clusterID, String addresses)
            throws InitializationException;

    private static native void clientInitEcho(ByteBuffer tb_client, byte[] clusterID,
            String addresses) throws InitializationException;

    private static native void clientDeinit(ByteBuffer tb_client);
}
