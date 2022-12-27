package com.tigerbeetle;

import java.lang.annotation.Native;
import java.nio.ByteBuffer;
import java.util.Objects;
import static com.tigerbeetle.AssertionError.assertTrue;

abstract class Request<TResponse extends Batch> {

    // @formatter:off
    /*
     * Overview:
     *
     * Implements a context that will be used to submit the request and to signal the completion.
     * A reference to this class is stored by the JNI side in the "user_data" field when calling "tb_client_submit",
     * meaning that no GC will occur before the callback completion
     *
     * Memory:
     *
     * - Holds the request body until the completion to be accessible by the C client.
     * - Copies the response body to be exposed to the application.
     *
     * Completion:
     *
     * - See AsyncRequest.java and BlockingRequest.java
     *
     */
    // @formatter:on

    enum Operations {
        CREATE_ACCOUNTS(3),
        CREATE_TRANSFERS(4),
        LOOKUP_ACCOUNTS(5),
        LOOKUP_TRANSFERS(6),

        ECHO_ACCOUNTS(3),
        ECHO_TRANSFERS(4);

        byte value;

        Operations(int value) {
            this.value = (byte) value;
        }
    }

    // Used ony by the JNI side
    @Native
    private final ByteBuffer buffer;

    @Native
    private final long bufferLen;

    private final NativeClient nativeClient;
    private final Operations operation;
    private final int requestLen;

    protected Request(final NativeClient nativeClient, final Operations operation,
            final Batch batch) {
        Objects.requireNonNull(nativeClient, "Client cannot be null");
        Objects.requireNonNull(batch, "Batch cannot be null");

        this.nativeClient = nativeClient;
        this.operation = operation;
        this.requestLen = batch.getLength();
        this.buffer = batch.getBuffer();
        this.bufferLen = batch.getBufferLen();

        if (this.bufferLen == 0 || this.requestLen == 0)
            throw new IllegalArgumentException("Empty batch");
    }

    public void beginRequest() {
        nativeClient.submit(this);
    }

    // Unchecked: Since we just support a limited set of operations, it is safe to cast the
    // result to T[]
    @SuppressWarnings("unchecked")
    void endRequest(final byte receivedOperation, final ByteBuffer buffer, final long packet,
            final byte status) {

        // This method is called from the JNI side, on the tb_client thread
        // We CAN'T throw any exception here, any event must be stored and
        // handled from the user's thread on the completion.

        Batch result = null;
        Throwable exception = null;

        try {

            if (receivedOperation != operation.value) {

                exception =
                        new AssertionError("Unexpected callback operation: expected=%d, actual=%d",
                                operation.value, receivedOperation);

            } else if (packet == 0) {

                exception = new AssertionError("Unexpected callback packet: packet=null");

            } else if (status != PacketStatus.Ok.value) {

                exception = new RequestException(status);

            } else if (buffer == null) {

                exception = new AssertionError("Unexpected callback buffer: buffer=null");

            } else {

                switch (operation) {
                    case CREATE_ACCOUNTS: {
                        result = buffer.capacity() == 0 ? CreateAccountResultBatch.EMPTY
                                : new CreateAccountResultBatch(memcpy(buffer));
                        break;
                    }

                    case CREATE_TRANSFERS: {
                        result = buffer.capacity() == 0 ? CreateTransferResultBatch.EMPTY
                                : new CreateTransferResultBatch(memcpy(buffer));
                        break;
                    }

                    case ECHO_ACCOUNTS:
                    case LOOKUP_ACCOUNTS: {
                        result = buffer.capacity() == 0 ? AccountBatch.EMPTY
                                : new AccountBatch(memcpy(buffer));
                        break;
                    }

                    case ECHO_TRANSFERS:
                    case LOOKUP_TRANSFERS: {
                        result = buffer.capacity() == 0 ? TransferBatch.EMPTY
                                : new TransferBatch(memcpy(buffer));
                        break;
                    }

                    default: {
                        exception = new AssertionError("Unknown operation %d", operation);
                        break;
                    }
                }
            }
        } catch (Throwable any) {
            exception = any;
        }

        if (exception != null) {
            setException(exception);
        } else {

            if (result.getLength() > requestLen) {

                setException(new AssertionError(
                        "Amount of results is greater than the amount of requests: resultLen=%d, requestLen=%d",
                        result.getLength(), requestLen));
            } else {
                setResult((TResponse) result);
            }
        }
    }

    byte getOperation() {
        return this.operation.value;
    }

    void releasePermit() {
        // Releasing the packet to be used by another thread
        nativeClient.releasePermit();
    }

    /**
     * Copies the message buffer memory to managed memory.
     */
    static ByteBuffer memcpy(final ByteBuffer source) {

        assertTrue(source != null, "Source buffer cannot be null");
        assertTrue(source.isDirect(), "Source buffer must be direct");

        final var capacity = source.capacity();
        assertTrue(capacity > 0, "Source buffer cannot be empty");

        final var copy = ByteBuffer.allocate(capacity);
        copy.put(source);

        return copy.position(0).asReadOnlyBuffer();
    }

    protected abstract void setResult(final TResponse result);

    protected abstract void setException(final Throwable exception);
}
