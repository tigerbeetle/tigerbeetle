package com.tigerbeetle;

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

    protected final static class Status {
        public final static byte UNINITIALIZED = -1;
        public final static byte OK = 0;
        public final static byte TOO_MUCH_DATA = 1;
        public final static byte INVALID_OPERATION = 2;
        public final static byte INVALID_DATA_SIZE = 3;
    }

    // Used only by the JNI side
    @SuppressWarnings("unused")
    private final Client client;

    // Used ony by the JNI side
    @SuppressWarnings("unused")
    private final ByteBuffer body;

    @SuppressWarnings("unused")
    private final long bodyLen;

    private Object result = null;
    private final byte operation;
    private byte status = Status.UNINITIALIZED;

    protected Request(Client client, byte operation, Batch batch) {
        this.client = client;
        this.operation = operation;
        this.body = batch.buffer;
        this.bodyLen = batch.getBufferLen();
    }

    public void endRequest(ByteBuffer buffer, byte status) {
        synchronized (this) {
            this.status = status;
            if (status == Status.OK) {
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
            }

            this.notify();
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
        return status != Status.UNINITIALIZED;
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
        if (status != Status.OK)
            throw new RequestException(status);

        return (T[]) result;
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
    public T[] get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

        if (!waitForCompletion(unit.convert(timeout, TimeUnit.MILLISECONDS)))
            throw new TimeoutException();

        try {
            return getResult();
        } catch (RequestException exception) {
            throw new ExecutionException(exception);
        }
    }
}
