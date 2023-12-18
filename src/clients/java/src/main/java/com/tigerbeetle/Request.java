package com.tigerbeetle;

import java.lang.annotation.Native;
import java.nio.ByteBuffer;
import java.util.Objects;

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
        // TODO Auto-generate these.
        CREATE_ACCOUNTS(128),
        CREATE_TRANSFERS(129),
        LOOKUP_ACCOUNTS(130),
        LOOKUP_TRANSFERS(131),
        GET_ACCOUNT_TRANSFERS(132),

        ECHO_ACCOUNTS(128),
        ECHO_TRANSFERS(129);

        byte value;

        Operations(int value) {
            this.value = (byte) value;
        }
    }

    // Used ony by the JNI side
    @Native
    private final ByteBuffer sendBuffer;

    @Native
    private final long sendBufferLen;

    @Native
    private byte[] replyBuffer;

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
        this.sendBuffer = batch.getBuffer();
        this.sendBufferLen = batch.getBufferLen();
        this.replyBuffer = null;

        if (this.sendBufferLen == 0 || this.requestLen == 0)
            throw new IllegalArgumentException("Empty batch");
    }

    public void beginRequest() throws ConcurrencyExceededException {
        nativeClient.submit(this);
    }

    // Unchecked: Since we just support a limited set of operations, it is safe to cast the
    // result to T[]
    @SuppressWarnings("unchecked")
    void endRequest(final byte receivedOperation, final byte status) {

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

            } else if (status != PacketStatus.Ok.value) {

                exception = new RequestException(status);

            } else {

                switch (operation) {
                    case CREATE_ACCOUNTS: {
                        result = replyBuffer == null ? CreateAccountResultBatch.EMPTY
                                : new CreateAccountResultBatch(ByteBuffer.wrap(replyBuffer));
                        checkResultLength(result);
                        break;
                    }

                    case CREATE_TRANSFERS: {
                        result = replyBuffer == null ? CreateTransferResultBatch.EMPTY
                                : new CreateTransferResultBatch(ByteBuffer.wrap(replyBuffer));
                        checkResultLength(result);
                        break;
                    }

                    case ECHO_ACCOUNTS:
                    case LOOKUP_ACCOUNTS: {
                        result = replyBuffer == null ? AccountBatch.EMPTY
                                : new AccountBatch(ByteBuffer.wrap(replyBuffer));
                        checkResultLength(result);
                        break;
                    }

                    case ECHO_TRANSFERS:
                    case LOOKUP_TRANSFERS: {
                        result = replyBuffer == null ? TransferBatch.EMPTY
                                : new TransferBatch(ByteBuffer.wrap(replyBuffer));
                        checkResultLength(result);
                        break;
                    }

                    case GET_ACCOUNT_TRANSFERS: {
                        result = replyBuffer == null ? TransferBatch.EMPTY
                                : new TransferBatch(ByteBuffer.wrap(replyBuffer));
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

        if (exception == null) {
            setResult((TResponse) result);
        } else {
            setException(exception);
        }
    }

    final void checkResultLength(Batch result) {
        if (result.getLength() > requestLen) {
            setException(new AssertionError(
                    "Amount of results is greater than the amount of requests: resultLen=%d, requestLen=%d",
                    result.getLength(), requestLen));
        }
    }

    // Unused: Used by unit tests.
    @SuppressWarnings("unused")
    void setReplyBuffer(byte[] buffer) {
        this.replyBuffer = buffer;
    }

    // Unused: Used by the JNI side.
    @SuppressWarnings("unused")
    byte getOperation() {
        return this.operation.value;
    }

    protected abstract void setResult(final TResponse result);

    protected abstract void setException(final Throwable exception);
}
