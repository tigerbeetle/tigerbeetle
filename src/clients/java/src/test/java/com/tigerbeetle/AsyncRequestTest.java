
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
import com.tigerbeetle.Request.Operations;

public class AsyncRequestTest {

    @Test
    public void testCreateAccountsRequestConstructor() {
        var client = getDummyClient();
        var batch = new AccountBatch(1);
        batch.add();

        var request = AsyncRequest.createAccounts(client, batch);
        assert request != null;
    }

    @Test
    public void testCreateTransfersRequestConstructor() {
        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        var request = AsyncRequest.createTransfers(client, batch);
        assert request != null;
    }

    @Test
    public void testLookupAccountsRequestConstructor() {
        var client = getDummyClient();
        var batch = new IdBatch(1);
        batch.add();

        var request = AsyncRequest.lookupAccounts(client, batch);
        assert request != null;
    }

    @Test
    public void testLookupTransfersRequestConstructor() {
        var client = getDummyClient();
        var batch = new IdBatch(1);
        batch.add();

        var request = AsyncRequest.lookupTransfers(client, batch);
        assert request != null;
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithClientNull() {
        var batch = new AccountBatch(1);
        batch.add();

        AsyncRequest.createAccounts(null, batch);
        assert false;
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithBatchNull() {
        var client = getDummyClient();
        AsyncRequest.createAccounts(client, null);
        assert false;
    }

    @Test
    public void testConstructorWithZeroCapacityBatch() {
        var client = getDummyClient();
        var batch = new AccountBatch(0);
        AsyncRequest.createAccounts(client, batch);
    }

    @Test
    public void testConstructorWithZeroItemsBatch() {
        var client = getDummyClient();
        var batch = new AccountBatch(1);
        AsyncRequest.createAccounts(client, batch);
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidOperation() throws Throwable {
        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();


        var dummyBuffer = ByteBuffer.allocate(CreateTransferResultBatch.Struct.SIZE);
        var callback = new CallbackSimulator<CreateTransferResultBatch>(
                AsyncRequest.createTransfers(client, batch),
                Request.Operations.LOOKUP_ACCOUNTS.value, dummyBuffer, PacketStatus.Ok.value, 250);

        CompletableFuture<CreateTransferResultBatch> future = callback.request.getFuture();
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
    public void testEndRequestWithUnknownOperation() throws Throwable {
        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        final byte UNKNOWN = 99;

        var dummyBuffer = ByteBuffer.allocate(CreateTransferResultBatch.Struct.SIZE);
        var callback =
                new CallbackSimulator<CreateTransferResultBatch>(
                        new AsyncRequest<CreateTransferResultBatch>(client,
                                Operations.CREATE_TRANSFERS, batch),
                        UNKNOWN, dummyBuffer, PacketStatus.Ok.value, 250);

        CompletableFuture<CreateTransferResultBatch> future = callback.request.getFuture();
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
    public void testEndRequestWithNullBuffer() throws Throwable {
        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        var callback = new CallbackSimulator<CreateTransferResultBatch>(
                AsyncRequest.createTransfers(client, batch),
                Request.Operations.CREATE_TRANSFERS.value, null, PacketStatus.Ok.value, 250);

        CompletableFuture<CreateTransferResultBatch> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        var result = future.get();
        assertEquals(0, result.getLength());
        assertNotNull(result.getHeader());
        assertTrue(result.getHeader().getTimestamp() != 0L);
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidBufferSize() throws Throwable {
        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();


        var invalidBuffer = ByteBuffer.allocate((CreateTransferResultBatch.Struct.SIZE * 2) - 1);
        var callback = new CallbackSimulator<CreateTransferResultBatch>(
                AsyncRequest.createTransfers(client, batch),
                Request.Operations.CREATE_TRANSFERS.value, invalidBuffer, PacketStatus.Ok.value,
                250);

        CompletableFuture<CreateTransferResultBatch> future = callback.request.getFuture();
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
        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        var dummyBuffer = ByteBuffer.allocate(CreateTransferResultBatch.Struct.SIZE);
        var callback = new CallbackSimulator<CreateTransferResultBatch>(
                AsyncRequest.createTransfers(client, batch),
                Request.Operations.CREATE_TRANSFERS.value, dummyBuffer,
                PacketStatus.TooMuchData.value, 250);

        CompletableFuture<CreateTransferResultBatch> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        try {
            future.join();
            assert false;
        } catch (CompletionException e) {

            assertTrue(e.getCause() instanceof RequestException);

            var requestException = (RequestException) e.getCause();
            assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());
        }
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithAmountOfResultsGreaterThanAmountOfRequests() throws Throwable {
        var client = getDummyClient();

        // A batch with only 1 item
        var batch = new AccountBatch(1);
        batch.add();

        // A reply with 2 items, while the batch had only 1 item
        var incorrectReply = ByteBuffer.allocate(CreateAccountResultBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);

        var callback = new CallbackSimulator<CreateAccountResultBatch>(
                AsyncRequest.createAccounts(client, batch),
                Request.Operations.CREATE_ACCOUNTS.value, incorrectReply, PacketStatus.Ok.value,
                250);

        CompletableFuture<CreateAccountResultBatch> future = callback.request.getFuture();
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
        var client = getDummyClient();
        var batch = new AccountBatch(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocate(CreateAccountResultBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100);
        dummyReplyBuffer.putInt(CreateAccountStatus.IdMustNotBeZero.value);
        dummyReplyBuffer.putInt(0);
        dummyReplyBuffer.putLong(101);
        dummyReplyBuffer.putInt(CreateAccountStatus.Exists.value);
        dummyReplyBuffer.putInt(0);

        var callback = new CallbackSimulator<CreateAccountResultBatch>(
                AsyncRequest.createAccounts(client, batch),
                Request.Operations.CREATE_ACCOUNTS.value, dummyReplyBuffer, PacketStatus.Ok.value,
                250);

        CompletableFuture<CreateAccountResultBatch> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        var result = future.get();
        assertEquals(2, result.getLength());
        assertNotNull(result.getHeader());
        assertTrue(result.getHeader().getTimestamp() != 0L);

        assertTrue(result.next());
        assertEquals(100, result.getTimestamp());
        assertEquals(CreateAccountStatus.IdMustNotBeZero, result.getStatus());

        assertTrue(result.next());
        assertEquals(101, result.getTimestamp());
        assertEquals(CreateAccountStatus.Exists, result.getStatus());
    }

    @Test
    public void testCreateTransferEndRequest() throws InterruptedException, ExecutionException {
        var client = getDummyClient();
        var batch = new TransferBatch(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocate(CreateTransferResultBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100);
        dummyReplyBuffer.putInt(CreateTransferStatus.IdMustNotBeZero.value);
        dummyReplyBuffer.putInt(0);
        dummyReplyBuffer.putLong(101);
        dummyReplyBuffer.putInt(CreateTransferStatus.Exists.value);
        dummyReplyBuffer.putInt(0);

        var callback = new CallbackSimulator<CreateTransferResultBatch>(
                AsyncRequest.createTransfers(client, batch),
                Request.Operations.CREATE_TRANSFERS.value, dummyReplyBuffer, PacketStatus.Ok.value,
                250);

        CompletableFuture<CreateTransferResultBatch> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        var result = future.get();
        assertEquals(2, result.getLength());
        assertNotNull(result.getHeader());
        assertTrue(result.getHeader().getTimestamp() != 0L);

        assertTrue(result.next());
        assertEquals(100, result.getTimestamp());
        assertEquals(CreateTransferStatus.IdMustNotBeZero, result.getStatus());

        assertTrue(result.next());
        assertEquals(101, result.getTimestamp());
        assertEquals(CreateTransferStatus.Exists, result.getStatus());
    }

    @Test
    public void testLookupAccountEndRequest() throws InterruptedException, ExecutionException {
        var client = getDummyClient();
        var batch = new IdBatch(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocate(AccountBatch.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(AccountBatch.Struct.SIZE).putLong(200).putLong(2000);

        var callback =
                new CallbackSimulator<AccountBatch>(AsyncRequest.lookupAccounts(client, batch),
                        Request.Operations.LOOKUP_ACCOUNTS.value, dummyReplyBuffer,
                        PacketStatus.Ok.value, 250);

        CompletableFuture<AccountBatch> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        var result = future.get();
        assertEquals(2, result.getLength());
        assertNotNull(result.getHeader());
        assertTrue(result.getHeader().getTimestamp() != 0L);

        assertTrue(result.next());
        assertEquals(100L, result.getId(UInt128.LeastSignificant));
        assertEquals(1000L, result.getId(UInt128.MostSignificant));

        assertTrue(result.next());
        assertEquals(200L, result.getId(UInt128.LeastSignificant));
        assertEquals(2000L, result.getId(UInt128.MostSignificant));
    }

    @Test
    public void testLookupTransferEndRequest() throws InterruptedException, ExecutionException {
        var client = getDummyClient();
        var batch = new IdBatch(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocate(TransferBatch.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(TransferBatch.Struct.SIZE).putLong(200).putLong(2000);

        var callback =
                new CallbackSimulator<TransferBatch>(AsyncRequest.lookupTransfers(client, batch),
                        Request.Operations.LOOKUP_TRANSFERS.value, dummyReplyBuffer,
                        PacketStatus.Ok.value, 250);

        CompletableFuture<TransferBatch> future = callback.request.getFuture();
        callback.start();
        assertFalse(future.isDone());

        var result = future.get();
        assertEquals(2, result.getLength());
        assertNotNull(result.getHeader());
        assertTrue(result.getHeader().getTimestamp() != 0L);

        assertTrue(result.next());
        assertEquals(100L, result.getId(UInt128.LeastSignificant));
        assertEquals(1000L, result.getId(UInt128.MostSignificant));

        assertTrue(result.next());
        assertEquals(200L, result.getId(UInt128.LeastSignificant));
        assertEquals(2000L, result.getId(UInt128.MostSignificant));
    }

    @Test
    public void testSuccessFuture() throws InterruptedException, ExecutionException {
        var client = getDummyClient();
        var batch = new IdBatch(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocate(TransferBatch.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(TransferBatch.Struct.SIZE).putLong(200).putLong(2000);

        var callback =
                new CallbackSimulator<TransferBatch>(AsyncRequest.lookupTransfers(client, batch),
                        Request.Operations.LOOKUP_TRANSFERS.value, dummyReplyBuffer,
                        PacketStatus.Ok.value, 5000);

        Future<TransferBatch> future = callback.request.getFuture();
        callback.start();

        try {
            // Our goal is just to test the future timeout.
            // The timeout is much smaller than the delay,
            // to avoid flaky results due to thread scheduling.
            future.get(5, TimeUnit.MILLISECONDS);
            assert false;

        } catch (TimeoutException timeout) {
            assert true;
        }

        // Wait for completion
        var result = future.get();
        assertEquals(2, result.getLength());
        assertNotNull(result.getHeader());
        assertTrue(result.getHeader().getTimestamp() != 0L);

        assertTrue(result.next());
        assertEquals(100L, result.getId(UInt128.LeastSignificant));
        assertEquals(1000L, result.getId(UInt128.MostSignificant));

        assertTrue(result.next());
        assertEquals(200L, result.getId(UInt128.LeastSignificant));
        assertEquals(2000L, result.getId(UInt128.MostSignificant));
    }

    @Test
    public void testSuccessFutureWithTimeout() throws InterruptedException, ExecutionException {
        var client = getDummyClient();
        var batch = new IdBatch(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocate(TransferBatch.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(TransferBatch.Struct.SIZE).putLong(200).putLong(2000);

        var callback =
                new CallbackSimulator<TransferBatch>(AsyncRequest.lookupTransfers(client, batch),
                        Request.Operations.LOOKUP_TRANSFERS.value, dummyReplyBuffer,
                        PacketStatus.Ok.value, 5);

        Future<TransferBatch> future = callback.request.getFuture();
        callback.start();

        try {

            // Our goal is just to test the future completion.
            // The timeout is much bigger than the delay,
            // to avoid flaky results due to thread scheduling.
            var result = future.get(5000, TimeUnit.MILLISECONDS);
            assertEquals(2, result.getLength());
            assertNotNull(result.getHeader());
            assertTrue(result.getHeader().getTimestamp() != 0L);

            assertTrue(result.next());
            assertEquals(100L, result.getId(UInt128.LeastSignificant));
            assertEquals(1000L, result.getId(UInt128.MostSignificant));

            assertTrue(result.next());
            assertEquals(200L, result.getId(UInt128.LeastSignificant));
            assertEquals(2000L, result.getId(UInt128.MostSignificant));

        } catch (TimeoutException timeout) {
            assert false;
        }
    }

    @Test
    public void testFailedFuture() throws InterruptedException {
        var client = getDummyClient();
        var batch = new IdBatch(1);
        batch.add();

        var callback =
                new CallbackSimulator<TransferBatch>(AsyncRequest.lookupTransfers(client, batch),
                        Request.Operations.LOOKUP_TRANSFERS.value, null,
                        PacketStatus.TooMuchData.value, 250);

        Future<TransferBatch> future = callback.request.getFuture();
        callback.start();

        try {
            future.get();
            assert false;
        } catch (ExecutionException exception) {

            assertTrue(exception.getCause() instanceof RequestException);

            var requestException = (RequestException) exception.getCause();
            assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());
        }
    }

    @Test
    public void testFailedFutureWithTimeout() throws InterruptedException, TimeoutException {
        var client = getDummyClient();
        var batch = new IdBatch(1);
        batch.add();

        var callback =
                new CallbackSimulator<AccountBatch>(AsyncRequest.lookupAccounts(client, batch),
                        Request.Operations.LOOKUP_ACCOUNTS.value, null,
                        PacketStatus.InvalidDataSize.value, 100);

        Future<AccountBatch> future = callback.request.getFuture();
        callback.start();

        try {
            future.get(1000, TimeUnit.MILLISECONDS);
            assert false;
        } catch (ExecutionException exception) {

            assertTrue(exception.getCause() instanceof RequestException);

            var requestException = (RequestException) exception.getCause();
            assertEquals(PacketStatus.InvalidDataSize.value, requestException.getStatus());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testFailedFutureCompletedTwice() {
        var client = getDummyClient();
        var batch = new IdBatch(1);
        batch.add();

        var request = AsyncRequest.lookupTransfers(client, batch);
        var status = PacketStatus.TooMuchData.value;

        try {
            // First completion is OK, registering the exception in the CompletableFuture.
            request.setException(new RequestException(status));
        } catch (Throwable any) {
            // No exception is expected in the first call.
            assert false;
        }
        // Second time throws an exception, because it can only be completed once.
        request.setException(new RequestException(status));

    }

    private static NativeClient getDummyClient() {
        return NativeClient.initEcho(UInt128.asBytes(0), "3000");
    }

    private class CallbackSimulator<T extends Batch> extends Thread {

        public final AsyncRequest<T> request;
        private final byte receivedOperation;
        private final ByteBuffer buffer;
        private final byte status;
        private final int delay;


        private CallbackSimulator(AsyncRequest<T> request, byte receivedOperation,
                ByteBuffer buffer, byte status, int delay) {
            this.request = request;
            this.receivedOperation = receivedOperation;
            this.buffer = buffer;
            this.status = status;
            this.delay = delay;
        }

        @Override
        public synchronized void run() {
            try {
                Thread.sleep(delay);
                if (buffer != null) {
                    request.setReplyBuffer(buffer.array());
                }
                request.endRequest(receivedOperation, status, System.nanoTime());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
