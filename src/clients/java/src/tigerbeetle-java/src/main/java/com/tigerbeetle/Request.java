package com.tigerbeetle;

import java.lang.annotation.Native;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

abstract class Request<T> implements Future<T[]> {

    protected final static class Operations {
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

    // This request must finish either with a result or an exception
    private Object result;

    protected Request(Client client, byte operation, Batch batch) throws IllegalArgumentException {
        this.client = client;
        this.operation = operation;
        this.requestLen = batch.getLenght();
        this.buffer = batch.getBuffer();
        this.bufferLen = batch.getBufferLen();

        this.result = null;

        if (this.bufferLen == 0 || this.requestLen == 0)
            throw new IllegalArgumentException("Empty batch");
    }

    public void beginRequest() {
        client.submit(this);
    }

    // Used only by the JNI side
    @SuppressWarnings("unused")
    private void endRequest(byte receivedOperation, ByteBuffer buffer, long packet, byte status) {

        // This method is called from the JNI side, on the tb_client thread
        // We CAN'T throw any exception here, any event must be stored and
        // handled from the user's thread on the completion.

        Object result = null;
        if (receivedOperation != operation) {

            result = new AssertionError("Unexpected callback operation: expected=%d, actual=%d",
                    operation, receivedOperation);

        } else if (buffer == null) {

            result = new AssertionError("Unexpected callback buffer: buffer=null");

        } else if (packet == 0) {

            result = new AssertionError("Unexpected callback packet: packet=null");

        } else if (status != RequestException.Status.OK) {

            result = new RequestException(status);

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

                // The amount of data received can be incorrect and cause a INVALID_DATA_SIZE
                result = any;
            }
        }

        client.returnPacket(packet);

        // Notify the waiting thread
        synchronized (this) {
            this.result = result;
            this.notifyAll();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // Canceling a request is not supported
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return result != null;
    }

    void waitForCompletionUninterruptibly() {
        try {
            waitForCompletion();
        } catch (InterruptedException interruptedException) {
            // Since we don't support canceling an ongoing request
            // this exception should never exposed by the API to be handled by the user
            throw new AssertionError(interruptedException,
                    "Unexpected thread interruption on waitForCompletion.");
        }
    }

    void waitForCompletion() throws InterruptedException {

        synchronized (this) {
            while (!isDone()) {
                wait();
            }
        }
    }

    boolean waitForCompletion(long timeoutMillis) throws InterruptedException {

        synchronized (this) {
            if (!isDone()) {
                wait(timeoutMillis);
                return isDone();
            } else {
                return true;
            }
        }
    }

    // Since we just support a limited set of operations, it is safe to cast the
    // result to T[]
    @SuppressWarnings("unchecked")
    T[] getResult() throws RequestException {

        if (result == null)
            throw new AssertionError("Unexpected request result: result=null");

        // Handling checked and unchecked exceptions accordingly
        if (result instanceof Throwable) {

            if (result instanceof RequestException)
                throw (RequestException) result;

            if (result instanceof RuntimeException)
                throw (RuntimeException) result;

            if (result instanceof Error)
                throw (Error) result;

            // If we can't determine the type of the exception,
            // throw a generic RuntimeException pointing as a cause
            throw new RuntimeException((Throwable) result);

        } else {

            var result = (T[]) this.result;

            if (result.length > requestLen)
                throw new AssertionError(
                        "Amount of results is greater than the amount of requests: resultLen=%d, requestLen=%d",
                        result.length, requestLen);

            return result;
        }
    }

    @Override
    public T[] get() throws InterruptedException, ExecutionException {

        waitForCompletion();

        try {
            return getResult();
        } catch (RequestException exception) {
            throw new ExecutionException(exception);
        }
    }

    @Override
    public T[] get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {

        if (!waitForCompletion(unit.convert(timeout, TimeUnit.MILLISECONDS)))
            throw new TimeoutException();

        try {
            return getResult();
        } catch (RequestException exception) {
            throw new ExecutionException(exception);
        }
    }
}
