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

    private final static byte UNINITIALIZED = -1;

    // Used ony by the JNI side
    @SuppressWarnings("unused")
    private final ByteBuffer body;

    @SuppressWarnings("unused")
    private final long bodyLen;

    private final Client client;
    private final byte operation;
    private Object result = null;
    private byte status =  UNINITIALIZED;

    protected Request(Client client, byte operation, Batch batch) {
        this.client = client;
        this.operation = operation;
        this.body = batch.buffer;
        this.bodyLen = batch.getBufferLen();
    }

    public void beginRequest() throws InterruptedException {

        long packet = client.adquirePacket();
        submit(client, packet);
    }

    void endRequest(ByteBuffer buffer, long packet, byte status) {

        Object result = null;
        if (status == RequestException.Status.OK) {
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

        client.returnPacket(packet);

        // Notify the waiting thread
        synchronized (this) {
            this.status = status;
            this.result = result;
            this.notifyAll();
        }

    }

    private native void submit(Client client, long packet);

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
        if (status != RequestException.Status.OK)
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
