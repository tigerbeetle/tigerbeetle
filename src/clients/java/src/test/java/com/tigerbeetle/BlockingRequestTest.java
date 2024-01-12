package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;
import com.tigerbeetle.Request.Operations;

public class BlockingRequestTest {

    @Test
    public void testCreateAccountsRequestConstructor() {

        var client = getDummyClient();
        var batch = new AccountBatch(1);
        batch.add();

        var request = BlockingRequest.createAccounts(client, batch);
        assert request != null;
    }

    @Test
    public void testCreateTransfersRequestConstructor() {

        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);
        assert request != null;
    }

    @Test
    public void testLookupAccountsRequestConstructor() {

        var client = getDummyClient();
        var batch = new IdBatch(1);
        batch.add();

        var request = BlockingRequest.lookupAccounts(client, batch);
        assert request != null;
    }

    @Test
    public void testLookupTransfersRequestConstructor() {

        var client = getDummyClient();
        var batch = new IdBatch(1);
        batch.add();

        var request = BlockingRequest.lookupTransfers(client, batch);
        assert request != null;
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithClientNull() {

        var batch = new AccountBatch(1);
        batch.add();

        BlockingRequest.createAccounts(null, batch);
        assert false;
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithBatchNull() {

        var client = getDummyClient();
        BlockingRequest.createAccounts(client, null);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroCapacityBatch() {

        var client = getDummyClient();
        var batch = new AccountBatch(0);

        BlockingRequest.createAccounts(client, batch);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroItemsBatch() {

        var client = getDummyClient();
        var batch = new AccountBatch(1);

        BlockingRequest.createAccounts(client, batch);
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidOperation() {

        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);

        assertFalse(request.isDone());

        // Invalid operation, should be CREATE_TRANSFERS
        request.endRequest(Request.Operations.LOOKUP_ACCOUNTS.value, PacketStatus.Ok.value);

        assertTrue(request.isDone());

        request.waitForResult();
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithUnknownOperation() {

        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        final byte UNKNOWN = 99;
        var request = new BlockingRequest<CreateTransferResultBatch>(client,
                Operations.CREATE_TRANSFERS, batch);

        assertFalse(request.isDone());

        // Unknown operation
        request.endRequest(UNKNOWN, PacketStatus.Ok.value);

        assertTrue(request.isDone());

        request.waitForResult();
        assert false;
    }

    @Test
    public void testEndRequestWithNullBuffer() {

        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);

        assertFalse(request.isDone());

        request.endRequest(Request.Operations.CREATE_TRANSFERS.value, PacketStatus.Ok.value);

        assertTrue(request.isDone());

        var result = request.waitForResult();
        assertEquals(0, result.getLength());
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidBufferSize() {

        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);
        var invalidBuffer = ByteBuffer.allocate((CreateTransferResultBatch.Struct.SIZE * 2) - 1);

        assertFalse(request.isDone());

        request.setReplyBuffer(invalidBuffer.array());
        request.endRequest(Request.Operations.CREATE_TRANSFERS.value, PacketStatus.Ok.value);

        assertTrue(request.isDone());

        request.waitForResult();
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testGetResultBeforeEndRequest() {

        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);

        assertFalse(request.isDone());

        request.getResult();
        assert false;
    }

    @Test
    public void testEndRequestWithRequestException() {

        var client = getDummyClient();
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);

        assertFalse(request.isDone());

        var dummyBuffer = ByteBuffer.allocate(CreateTransferResultBatch.Struct.SIZE);
        request.setReplyBuffer(dummyBuffer.array());
        request.endRequest(Request.Operations.CREATE_TRANSFERS.value,
                PacketStatus.TooMuchData.value);

        assertTrue(request.isDone());

        try {
            request.waitForResult();
            assert false;
        } catch (RequestException e) {
            assertEquals(PacketStatus.TooMuchData.value, e.getStatus());
        }
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithAmountOfResultsGreaterThanAmountOfRequests() {
        var client = getDummyClient();

        // A batch with only 1 item
        var batch = new AccountBatch(1);
        batch.add();

        // A reply with 2 items, while the batch had only 1 item
        var dummyReplyBuffer = ByteBuffer.allocate(CreateAccountResultBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);

        var request = BlockingRequest.createAccounts(client, batch);
        request.setReplyBuffer(dummyReplyBuffer.position(0).array());
        request.endRequest(Request.Operations.CREATE_ACCOUNTS.value, PacketStatus.Ok.value);

        assertTrue(request.isDone());
        request.waitForResult();
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestTwice() {
        var client = getDummyClient();

        // A batch with only 1 item
        var batch = new AccountBatch(1);
        batch.add();

        // A reply with 2 items, while the batch had only 1 item
        var dummyReplyBuffer = ByteBuffer.allocate(CreateAccountResultBatch.Struct.SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);

        var request = BlockingRequest.createAccounts(client, batch);
        assertFalse(request.isDone());

        request.setReplyBuffer(dummyReplyBuffer.position(0).array());
        request.endRequest(Request.Operations.CREATE_ACCOUNTS.value, PacketStatus.Ok.value);

        assertTrue(request.isDone());
        var result = request.waitForResult();
        assertEquals(1, result.getLength());

        request.setReplyBuffer(dummyReplyBuffer.position(0).array());
        request.endRequest(Request.Operations.CREATE_ACCOUNTS.value, PacketStatus.Ok.value);

        assertTrue(request.isDone());

        request.waitForResult();
        assert false;
    }

    @Test
    public void testCreateAccountEndRequest() {
        var client = getDummyClient();
        var batch = new AccountBatch(2);
        batch.add();
        batch.add();

        var request = BlockingRequest.createAccounts(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocate(CreateAccountResultBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putInt(0);
        dummyReplyBuffer.putInt(CreateAccountResult.IdMustNotBeZero.ordinal());
        dummyReplyBuffer.putInt(1);
        dummyReplyBuffer.putInt(CreateAccountResult.Exists.ordinal());

        request.setReplyBuffer(dummyReplyBuffer.position(0).array());
        request.endRequest(Request.Operations.CREATE_ACCOUNTS.value, PacketStatus.Ok.value);

        assertTrue(request.isDone());
        var result = request.waitForResult();
        assertEquals(2, result.getLength());

        assertTrue(result.next());
        assertEquals(0, result.getIndex());
        assertEquals(CreateAccountResult.IdMustNotBeZero, result.getResult());

        assertTrue(result.next());
        assertEquals(1, result.getIndex());
        assertEquals(CreateAccountResult.Exists, result.getResult());
    }

    @Test
    public void testCreateTransferEndRequest() {
        var client = getDummyClient();
        var batch = new TransferBatch(2);
        batch.add();
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocate(CreateTransferResultBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putInt(0);
        dummyReplyBuffer.putInt(CreateTransferResult.IdMustNotBeZero.ordinal());
        dummyReplyBuffer.putInt(1);
        dummyReplyBuffer.putInt(CreateTransferResult.Exists.ordinal());

        request.setReplyBuffer(dummyReplyBuffer.position(0).array());
        request.endRequest(Request.Operations.CREATE_TRANSFERS.value, PacketStatus.Ok.value);

        assertTrue(request.isDone());
        var result = request.waitForResult();
        assertEquals(2, result.getLength());

        assertTrue(result.next());
        assertEquals(0, result.getIndex());
        assertEquals(CreateTransferResult.IdMustNotBeZero, result.getResult());

        assertTrue(result.next());
        assertEquals(1, result.getIndex());
        assertEquals(CreateTransferResult.Exists, result.getResult());
    }

    @Test
    public void testLookupAccountEndRequest() {
        var client = getDummyClient();
        var batch = new IdBatch(2);
        batch.add();
        batch.add();

        var request = BlockingRequest.lookupAccounts(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocate(AccountBatch.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(AccountBatch.Struct.SIZE).putLong(200).putLong(2000);

        request.setReplyBuffer(dummyReplyBuffer.position(0).array());
        request.endRequest(Request.Operations.LOOKUP_ACCOUNTS.value, PacketStatus.Ok.value);

        assertTrue(request.isDone());
        var result = request.waitForResult();
        assertEquals(2, result.getLength());

        assertTrue(result.next());
        assertEquals(100L, result.getId(UInt128.LeastSignificant));
        assertEquals(1000L, result.getId(UInt128.MostSignificant));

        assertTrue(result.next());
        assertEquals(200L, result.getId(UInt128.LeastSignificant));
        assertEquals(2000L, result.getId(UInt128.MostSignificant));
    }

    @Test
    public void testLookupTransferEndRequest() {
        var client = getDummyClient();
        var batch = new IdBatch(2);
        batch.add();
        batch.add();

        var request = BlockingRequest.lookupTransfers(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocate(TransferBatch.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(TransferBatch.Struct.SIZE).putLong(200).putLong(2000);

        request.setReplyBuffer(dummyReplyBuffer.position(0).array());
        request.endRequest(Request.Operations.LOOKUP_TRANSFERS.value, PacketStatus.Ok.value);

        assertTrue(request.isDone());
        var result = request.waitForResult();
        assertEquals(2, result.getLength());

        assertTrue(result.next());
        assertEquals(100L, result.getId(UInt128.LeastSignificant));
        assertEquals(1000L, result.getId(UInt128.MostSignificant));

        assertTrue(result.next());
        assertEquals(200L, result.getId(UInt128.LeastSignificant));
        assertEquals(2000L, result.getId(UInt128.MostSignificant));
    }

    @Test
    public void testSuccessCompletion() {

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
                new CallbackSimulator<TransferBatch>(BlockingRequest.lookupTransfers(client, batch),
                        Request.Operations.LOOKUP_TRANSFERS.value, dummyReplyBuffer,
                        PacketStatus.Ok.value, 500);

        callback.start();

        // Wait for completion
        var result = callback.request.waitForResult();
        assertEquals(2, result.getLength());

        assertTrue(result.next());
        assertEquals(100L, result.getId(UInt128.LeastSignificant));
        assertEquals(1000L, result.getId(UInt128.MostSignificant));

        assertTrue(result.next());
        assertEquals(200L, result.getId(UInt128.LeastSignificant));
        assertEquals(2000L, result.getId(UInt128.MostSignificant));
    }

    @Test
    public void testFailedCompletion() throws InterruptedException {

        var client = getDummyClient();
        var batch = new IdBatch(1);
        batch.add();

        var callback =
                new CallbackSimulator<TransferBatch>(BlockingRequest.lookupTransfers(client, batch),
                        Request.Operations.LOOKUP_TRANSFERS.value, null,
                        PacketStatus.TooMuchData.value, 250);

        callback.start();

        try {
            callback.request.waitForResult();
            assert false;
        } catch (RequestException requestException) {

            assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());
        }

    }

    private static NativeClient getDummyClient() {
        return NativeClient.initEcho(UInt128.asBytes(0), "3000", 1);
    }

    private class CallbackSimulator<T extends Batch> extends Thread {

        public final BlockingRequest<T> request;
        private final byte receivedOperation;
        private final ByteBuffer buffer;
        private final byte status;
        private final int delay;


        private CallbackSimulator(BlockingRequest<T> request, byte receivedOperation,
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
                request.endRequest(receivedOperation, status);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
