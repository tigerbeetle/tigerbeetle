package com.tigerbeetle;

import java.nio.ByteBuffer;

class Request {

    public final static class Operations {
        public final static byte CREATE_ACCOUNTS = 3;
        public final static byte CREATE_TRANSFERS = 4;
        public final static byte LOOKUP_ACCOUNTS = 5;
        public final static byte LOOKUP_TRANSFERS = 6;
    }

    private final static class Status {
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

    public Request(Client client, byte operation, Batch batch) {
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
                        result = null;
                        break;
                    }

                    case Operations.LOOKUP_ACCOUNTS: {
                        var batch = new AccountsBatch(buffer.asReadOnlyBuffer());
                        result = batch.toArray();
                        break;
                    }

                    case Operations.LOOKUP_TRANSFERS: {
                        result = null;
                        break;
                    }
                }
            }

            this.notify();
        }
    }

    public Object waitForResult() throws Exception, InterruptedException {
        synchronized (this) {
            while (result == null && status == Status.UNINITIALIZED) {
                wait();
            }

            if (result == null)
                throw new Exception("Result = " + status);
            return result;
        }
    }
}
