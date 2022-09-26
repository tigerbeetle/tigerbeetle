package com.tigerbeetle;

import java.lang.annotation.Native;
import java.nio.ByteBuffer;
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

    interface Operations {
        public final static byte CREATE_ACCOUNTS = 3;
        public final static byte CREATE_TRANSFERS = 4;
        public final static byte LOOKUP_ACCOUNTS = 5;
        public final static byte LOOKUP_TRANSFERS = 6;
    }

    // Used ony by the JNI side
    @Native
    private final ByteBuffer buffer;

    @Native
    private final long bufferLen;

    private final Client client;
    private final byte operation;
    private final int requestLen;

    protected Request(final Client client, final byte operation, final Batch batch) {

        if (client == null)
            throw new NullPointerException("Client cannot be null");
        if (batch == null)
            throw new NullPointerException("Batch cannot be null");

        this.client = client;
        this.operation = operation;
        this.requestLen = batch.getLength();
        this.buffer = batch.getBuffer();
        this.bufferLen = batch.getBufferLen();

        if (this.bufferLen == 0 || this.requestLen == 0)
            throw new IllegalArgumentException("Empty batch");
    }

    public void beginRequest() {
        client.submit(this);
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

        if (receivedOperation != operation) {

            exception = new AssertionError("Unexpected callback operation: expected=%d, actual=%d",
                    operation, receivedOperation);

        } else if (packet == 0) {

            exception = new AssertionError("Unexpected callback packet: packet=null");

        } else if (status != RequestException.Status.OK) {

            exception = new RequestException(status);

        } else if (buffer == null) {

            exception = new AssertionError("Unexpected callback buffer: buffer=null");

        } else {

            try {
                switch (operation) {
                    case Operations.CREATE_ACCOUNTS: {
                        result = buffer.capacity() == 0 ? CreateAccountResults.EMPTY
                                : new CreateAccountResults(memcpy(buffer));
                        break;
                    }

                    case Operations.CREATE_TRANSFERS: {
                        result = buffer.capacity() == 0 ? CreateTransferResults.EMPTY
                                : new CreateTransferResults(memcpy(buffer));
                        break;
                    }

                    case Operations.LOOKUP_ACCOUNTS: {
                        result = buffer.capacity() == 0 ? Accounts.EMPTY
                                : new Accounts(memcpy(buffer));
                        break;
                    }

                    case Operations.LOOKUP_TRANSFERS: {
                        result = buffer.capacity() == 0 ? Transfers.EMPTY
                                : new Transfers(memcpy(buffer));
                        break;
                    }

                    default: {
                        exception = new AssertionError("Unknown operation %d", operation);
                        break;
                    }
                }
            } catch (Throwable any) {

                exception = any;
            }
        }

        client.returnPacket(packet);

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


    /**
     * Copies the message buffer memory to managed memory.
     */
    private ByteBuffer memcpy(final ByteBuffer source) {

        assertTrue(source != null, "Source buffer cannot be null");
        assertTrue(source.isDirect(), "Source buffer must be direct");

        final var capacity = source.capacity();
        assertTrue(capacity >= 0, "Source buffer cannot be empty");

        final var copy = ByteBuffer.allocate(capacity);
        copy.put(source);

        return copy.position(0).asReadOnlyBuffer();
    }

    protected abstract void setResult(final TResponse result);

    protected abstract void setException(final Throwable exception);
}
