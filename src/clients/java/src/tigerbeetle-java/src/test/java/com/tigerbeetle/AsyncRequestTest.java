
package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import com.tigerbeetle.UInt128.Bytes;

public class AsyncRequestTest {

    @Test
    public void testCreateAccountsRequestConstructor() {

        var client = new Client(0, 1);
        var batch = new Accounts(1);
        batch.add();

        var request = AsyncRequest.createAccounts(client, batch);
        assert request != null;
    }

    @Test
    public void testCreateTransfersRequestConstructor() {

        var client = new Client(0, 1);
        var batch = new Transfers(1);
        batch.add();

        var request = AsyncRequest.createTransfers(client, batch);
        assert request != null;
    }

    @Test
    public void testLookupAccountsRequestConstructor() {

        var client = new Client(0, 1);
        var batch = new Ids(1);
        batch.add();

        var request = AsyncRequest.lookupAccounts(client, batch);
        assert request != null;
    }

    @Test
    public void testLookupTransfersRequestConstructor() {

        var client = new Client(0, 1);
        var batch = new Ids(1);
        batch.add();

        var request = AsyncRequest.lookupTransfers(client, batch);
        assert request != null;
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithClientNull() {

        var batch = new Accounts(1);
        batch.add();

        AsyncRequest.createAccounts(null, batch);
        assert false;
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithBatchNull() {

        var client = new Client(0, 1);
        AsyncRequest.createAccounts(client, null);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroCapacityBatch() {

        var client = new Client(0, 1);
        var batch = new Accounts(0);

        AsyncRequest.createAccounts(client, batch);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroItemsBatch() {

        var client = new Client(0, 1);
        var batch = new Accounts(1);

        AsyncRequest.createAccounts(client, batch);
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidOperation() throws Throwable {

        var client = new Client(0, 1);
        var batch = new Transfers(1);
        batch.add();


        var dummyBuffer = ByteBuffer.allocateDirect(CreateTransferResults.Struct.SIZE);
        var callback = new CallbackSimulator<CreateTransferResults>(
                AsyncRequest.createTransfers(client, batch), Request.Operations.LOOKUP_ACCOUNTS,
                dummyBuffer, 1, RequestException.Status.OK, 250);

        CompletableFuture<CreateTransferResults> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        try {
            future.get();
            assert false;
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
            throw e.getCause();
        }
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithNullBuffer() throws Throwable {

        var client = new Client(0, 1);
        var batch = new Transfers(1);
        batch.add();

        var callback = new CallbackSimulator<CreateTransferResults>(
                AsyncRequest.createTransfers(client, batch), Request.Operations.CREATE_TRANSFERS,
                null, 1, RequestException.Status.OK, 250);

        CompletableFuture<CreateTransferResults> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        try {
            future.get();
            assert false;
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
            throw e.getCause();
        }
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidBufferSize() throws Throwable {

        var client = new Client(0, 1);
        var batch = new Transfers(1);
        batch.add();


        var invalidBuffer = ByteBuffer.allocateDirect((CreateTransferResults.Struct.SIZE * 2) - 1);
        var callback = new CallbackSimulator<CreateTransferResults>(
                AsyncRequest.createTransfers(client, batch), Request.Operations.CREATE_TRANSFERS,
                invalidBuffer, 1, RequestException.Status.OK, 250);

        CompletableFuture<CreateTransferResults> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        try {
            future.get();
            assert false;
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
            throw e.getCause();
        }
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidPacket() throws Throwable {

        var client = new Client(0, 1);
        var batch = new Transfers(1);
        batch.add();

        var dummyBuffer = ByteBuffer.allocateDirect(Transfers.Struct.SIZE);
        final long NULL = 0; // Packet is a long representing a pointer, cannot be null

        var callback = new CallbackSimulator<CreateTransferResults>(
                AsyncRequest.createTransfers(client, batch), Request.Operations.CREATE_TRANSFERS,
                dummyBuffer, NULL, RequestException.Status.OK, 250);

        CompletableFuture<CreateTransferResults> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        try {
            future.get();
            assert false;
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
            throw e.getCause();
        }
    }

    @Test
    public void testEndRequestWithRequestException() {

        var client = new Client(0, 1);
        var batch = new Transfers(1);
        batch.add();

        var dummyBuffer = ByteBuffer.allocateDirect(CreateTransferResults.Struct.SIZE);
        var callback = new CallbackSimulator<CreateTransferResults>(
                AsyncRequest.createTransfers(client, batch), Request.Operations.CREATE_TRANSFERS,
                dummyBuffer, 1, RequestException.Status.TOO_MUCH_DATA, 250);

        CompletableFuture<CreateTransferResults> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        try {
            future.join();
            assert false;
        } catch (CompletionException e) {

            assertTrue(e.getCause() instanceof RequestException);

            var requestException = (RequestException) e.getCause();
            assertEquals(RequestException.Status.TOO_MUCH_DATA, requestException.getStatus());
        }
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithAmountOfResultsGreaterThanAmountOfRequests() throws Throwable {
        var client = new Client(0, 1);

        // A batch with only 1 item
        var batch = new Accounts(1);
        batch.add();

        // A reply with 2 items, while the batch had only 1 item
        var incorrectReply = ByteBuffer.allocateDirect(CreateAccountResults.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);

        var callback = new CallbackSimulator<CreateAccountResults>(
                AsyncRequest.createAccounts(client, batch), Request.Operations.CREATE_ACCOUNTS,
                incorrectReply.position(0), 1, RequestException.Status.OK, 250);

        CompletableFuture<CreateAccountResults> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        try {
            future.get();
            assert false;
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
            throw e.getCause();
        }
    }

    @Test
    public void testCreateAccountEndRequest() throws ExecutionException, InterruptedException {
        var client = new Client(0, 1);
        var batch = new Accounts(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(CreateAccountResults.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putInt(0);
        dummyReplyBuffer.putInt(CreateAccountResult.IdMustNotBeZero.ordinal());
        dummyReplyBuffer.putInt(1);
        dummyReplyBuffer.putInt(CreateAccountResult.Exists.ordinal());

        var callback = new CallbackSimulator<CreateAccountResults>(
                AsyncRequest.createAccounts(client, batch), Request.Operations.CREATE_ACCOUNTS,
                dummyReplyBuffer.position(0), 1, RequestException.Status.OK, 250);

        CompletableFuture<CreateAccountResults> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        var result = future.get();
        assertEquals(2, result.getLength());

        assertTrue(result.next());
        assertEquals(0, result.getIndex());
        assertEquals(CreateAccountResult.IdMustNotBeZero, result.getResult());

        assertTrue(result.next());
        assertEquals(1, result.getIndex());
        assertEquals(CreateAccountResult.Exists, result.getResult());
    }

    @Test
    public void testCreateTransferEndRequest() throws InterruptedException, ExecutionException {
        var client = new Client(0, 1);
        var batch = new Transfers(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(CreateTransferResults.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putInt(0);
        dummyReplyBuffer.putInt(CreateTransferResult.IdMustNotBeZero.ordinal());
        dummyReplyBuffer.putInt(1);
        dummyReplyBuffer.putInt(CreateTransferResult.Exists.ordinal());

        var callback = new CallbackSimulator<CreateTransferResults>(
                AsyncRequest.createTransfers(client, batch), Request.Operations.CREATE_TRANSFERS,
                dummyReplyBuffer.position(0), 1, RequestException.Status.OK, 250);

        CompletableFuture<CreateTransferResults> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        var result = future.get();
        assertEquals(2, result.getLength());

        assertTrue(result.next());
        assertEquals(0, result.getIndex());
        assertEquals(CreateTransferResult.IdMustNotBeZero, result.getResult());

        assertTrue(result.next());
        assertEquals(1, result.getIndex());
        assertEquals(CreateTransferResult.Exists, result.getResult());
    }

    @Test
    public void testLookupAccountEndRequest() throws InterruptedException, ExecutionException {
        var client = new Client(0, 1);
        var batch = new Ids(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocateDirect(Accounts.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(Accounts.Struct.SIZE).putLong(200).putLong(2000);

        var callback = new CallbackSimulator<Accounts>(AsyncRequest.lookupAccounts(client, batch),
                Request.Operations.LOOKUP_ACCOUNTS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.OK, 250);

        CompletableFuture<Accounts> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        var result = future.get();
        assertEquals(2, result.getLength());

        assertTrue(result.next());
        assertEquals(100L, result.getId(Bytes.LeastSignificant));
        assertEquals(1000L, result.getId(Bytes.MostSignificant));

        assertTrue(result.next());
        assertEquals(200L, result.getId(Bytes.LeastSignificant));
        assertEquals(2000L, result.getId(Bytes.MostSignificant));
    }

    @Test
    public void testLookupTransferEndRequest() throws InterruptedException, ExecutionException {
        var client = new Client(0, 1);
        var batch = new Ids(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocateDirect(Transfers.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(Transfers.Struct.SIZE).putLong(200).putLong(2000);

        var callback = new CallbackSimulator<Transfers>(AsyncRequest.lookupTransfers(client, batch),
                Request.Operations.LOOKUP_TRANSFERS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.OK, 250);

        CompletableFuture<Transfers> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        var result = future.get();
        assertEquals(2, result.getLength());

        assertTrue(result.next());
        assertEquals(100L, result.getId(Bytes.LeastSignificant));
        assertEquals(1000L, result.getId(Bytes.MostSignificant));

        assertTrue(result.next());
        assertEquals(200L, result.getId(Bytes.LeastSignificant));
        assertEquals(2000L, result.getId(Bytes.MostSignificant));
    }

    @Test
    public void testSuccessFuture() throws InterruptedException, ExecutionException {

        var client = new Client(0, 1);
        var batch = new Ids(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocateDirect(Transfers.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(Transfers.Struct.SIZE).putLong(200).putLong(2000);

        var callback = new CallbackSimulator<Transfers>(AsyncRequest.lookupTransfers(client, batch),
                Request.Operations.LOOKUP_TRANSFERS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.OK, 500);

        Future<Transfers> future = callback.request.getFuture();
        callback.start();

        try {
            // Should not be ready yet
            future.get(5, TimeUnit.MILLISECONDS);
            assert false;

        } catch (TimeoutException timeout) {
            assert true;
        }

        // Wait for completion
        var result = future.get();
        assertEquals(2, result.getLength());

        assertTrue(result.next());
        assertEquals(100L, result.getId(Bytes.LeastSignificant));
        assertEquals(1000L, result.getId(Bytes.MostSignificant));

        assertTrue(result.next());
        assertEquals(200L, result.getId(Bytes.LeastSignificant));
        assertEquals(2000L, result.getId(Bytes.MostSignificant));
    }

    @Test
    public void testSuccessFutureWithTimeout() throws InterruptedException, ExecutionException {

        var client = new Client(0, 1);
        var batch = new Ids(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocateDirect(Transfers.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(Transfers.Struct.SIZE).putLong(200).putLong(2000);

        var callback = new CallbackSimulator<Transfers>(AsyncRequest.lookupTransfers(client, batch),
                Request.Operations.LOOKUP_TRANSFERS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.OK, 500);

        Future<Transfers> future = callback.request.getFuture();
        callback.start();

        try {

            var result = future.get(1000, TimeUnit.MILLISECONDS);
            assertEquals(2, result.getLength());

            assertTrue(result.next());
            assertEquals(100L, result.getId(Bytes.LeastSignificant));
            assertEquals(1000L, result.getId(Bytes.MostSignificant));

            assertTrue(result.next());
            assertEquals(200L, result.getId(Bytes.LeastSignificant));
            assertEquals(2000L, result.getId(Bytes.MostSignificant));

        } catch (TimeoutException timeout) {
            assert false;
        }
    }


    @Test
    public void testFailedFuture() throws InterruptedException {

        var client = new Client(0, 1);
        var batch = new Ids(1);
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(0);

        var callback = new CallbackSimulator<Transfers>(AsyncRequest.lookupTransfers(client, batch),
                Request.Operations.LOOKUP_TRANSFERS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.TOO_MUCH_DATA, 250);

        Future<Transfers> future = callback.request.getFuture();
        callback.start();

        try {
            future.get();
            assert false;
        } catch (ExecutionException exception) {

            assertTrue(exception.getCause() instanceof RequestException);

            var requestException = (RequestException) exception.getCause();
            assertEquals(RequestException.Status.TOO_MUCH_DATA, requestException.getStatus());
        }

    }

    @Test
    public void testFailedFutureWithTimeout() throws InterruptedException, TimeoutException {

        var client = new Client(0, 1);
        var batch = new Ids(1);
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(0);

        var callback = new CallbackSimulator<Accounts>(AsyncRequest.lookupAccounts(client, batch),
                Request.Operations.LOOKUP_ACCOUNTS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.INVALID_DATA_SIZE, 100);

        Future<Accounts> future = callback.request.getFuture();
        callback.start();

        try {
            future.get(1000, TimeUnit.MILLISECONDS);
            assert false;
        } catch (ExecutionException exception) {

            assertTrue(exception.getCause() instanceof RequestException);

            var requestException = (RequestException) exception.getCause();
            assertEquals(RequestException.Status.INVALID_DATA_SIZE, requestException.getStatus());
        }
    }


    private class CallbackSimulator<T extends Batch> extends Thread {

        public final AsyncRequest<T> request;
        private final byte receivedOperation;
        private final ByteBuffer buffer;
        private final long packet;
        private final byte status;
        private final int delay;


        private CallbackSimulator(AsyncRequest<T> request, byte receivedOperation,
                ByteBuffer buffer, long packet, byte status, int delay) {
            this.request = request;
            this.receivedOperation = receivedOperation;
            this.buffer = buffer;
            this.packet = packet;
            this.status = status;
            this.delay = delay;
        }

        @Override
        public synchronized void run() {
            try {
                Thread.sleep(delay);
                request.endRequest(receivedOperation, buffer, packet, status);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

