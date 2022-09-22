package com.tigerbeetle;

import java.lang.annotation.Native;
import java.nio.ByteBuffer;

abstract class Request<T> {

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

    protected Request(Client client, byte operation, Batch batch) {

        if (client == null)
            throw new NullPointerException("Client cannot be null");
        if (batch == null)
            throw new NullPointerException("Batch cannot be null");

        this.client = client;
        this.operation = operation;
        this.requestLen = batch.getLenght();
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
    void endRequest(byte receivedOperation, ByteBuffer buffer, long packet, byte status) {

        // This method is called from the JNI side, on the tb_client thread
        // We CAN'T throw any exception here, any event must be stored and
        // handled from the user's thread on the completion.

        Object result = null;

        if (receivedOperation != operation) {

            result = new AssertionError("Unexpected callback operation: expected=%d, actual=%d",
                    operation, receivedOperation);

        } else if (packet == 0) {

            result = new AssertionError("Unexpected callback packet: packet=null");

        } else if (status != RequestException.Status.OK) {

            result = new RequestException(status);

        } else if (buffer == null) {

            result = new AssertionError("Unexpected callback buffer: buffer=null");

        } else {

            try {
                switch (operation) {
                    case Operations.CREATE_ACCOUNTS: {
                        var batch = new CreateAccountsResultBatch(buffer.asReadOnlyBuffer());
                        result = batch.toArray();
                        break;
                    }

                    case Operations.CREATE_TRANSFERS: {
                        var batch = new CreateTransfersResultBatch(buffer.asReadOnlyBuffer());
                        result = batch.toArray();
                        break;
                    }

                    case Operations.LOOKUP_ACCOUNTS: {
                        var batch = new AccountsBatch(buffer.asReadOnlyBuffer());
                        result = batch.toArray();
                        break;
                    }
                    case Operations.LOOKUP_TRANSFERS: {
                        var batch = new TransfersBatch(buffer.asReadOnlyBuffer());
                        result = batch.toArray();
                        break;
                    }
                }
            } catch (Throwable any) {

                result = any;
            }
        }

        client.returnPacket(packet);

        if (result instanceof Throwable) {
            setException((Throwable) result);
        } else {

            var array = (T[]) result;

            if (array.length > requestLen) {

                setException(new AssertionError(
                        "Amount of results is greater than the amount of requests: resultLen=%d, requestLen=%d",
                        array.length, requestLen));
            } else {
                setResult(array);
            }
        }
    }

    protected abstract void setResult(T[] result);

    protected abstract void setException(Throwable exception);
}
