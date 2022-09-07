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

    private final static byte UNINITIALIZED = -1;

    // Used ony by the JNI side
    @Native
    private final ByteBuffer buffer;

    @Native
    private final long bufferLen;

    private final Client client;
    private final byte operation;
    private final int requestLen;
    private Object result = null;
    private byte status = UNINITIALIZED;

    protected Request(Client client, byte operation, Batch batch)
            throws IllegalArgumentException {
        this.client = client;
        this.operation = operation;
        this.requestLen = batch.getLenght();
        this.buffer = batch.getBuffer();
        this.bufferLen = batch.getBufferLen();

        if (this.bufferLen == 0 || this.requestLen == 0)
            throw new IllegalArgumentException("Empty batch");
    }

    public void beginRequest()
            throws InterruptedException {
        client.submit(this);
    }

    void endRequest(byte receivedOperation, ByteBuffer buffer, long packet, byte status) {

        // This method is called from the JNI side, on the tb_client thread
        // We don't want to throw any exception here, any event must be stored and
        // handled from the user's thread

        Object result = null;
        if (receivedOperation != operation) {

            // This is a protocol error,
            // it is expected to receive the same operation on the reply
            status = RequestException.Status.INVALID_OPERATION;

        } else if (status == RequestException.Status.OK) {

            if (buffer == null)
                throw new IllegalArgumentException("buffer");

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
            } catch (RequestException requestException) {

                // The amount of data received can be incorrect and cause a INVALID_DATA_SIZE
                status = requestException.getStatus();
                result = null;
            }
        }

        client.returnPacket(packet);

        // Notify the waiting thread
        synchronized (this) {
            this.status = status;
            this.result = result;
            this.notifyAll();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // Cancel a tigerbeetle request is not supported
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return status != UNINITIALIZED;
    }

    void waitForCompletion()
            throws InterruptedException {
        synchronized (this) {
            while (!isDone()) {
                wait();
            }
        }
    }

    boolean waitForCompletion(long timeoutMillis)
            throws InterruptedException {
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
    T[] getResult()
            throws RequestException {
        if (status != RequestException.Status.OK)
            throw new RequestException(status);

        if (result == null)
            throw new IllegalStateException("Null result is unexpected when Status=OK");

        var result = (T[]) this.result;

        // Make sure the amount of results at least matches the amount of requests
        if (result.length > requestLen)
            throw new RequestException(RequestException.Status.INVALID_DATA_SIZE);

        return result;
    }

    @Override
    public T[] get()
            throws InterruptedException, ExecutionException {

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
