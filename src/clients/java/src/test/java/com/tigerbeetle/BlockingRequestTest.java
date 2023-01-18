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

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new AccountBatch(1);
        batch.add();

        var request = BlockingRequest.createAccounts(client, batch);
        assert request != null;
    }

    @Test
    public void testCreateTransfersRequestConstructor() {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);
        assert request != null;
    }

    @Test
    public void testLookupAccountsRequestConstructor() {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new IdBatch(1);
        batch.add();

        var request = BlockingRequest.lookupAccounts(client, batch);
        assert request != null;
    }

    @Test
    public void testLookupTransfersRequestConstructor() {

        var client = NativeClient.initEcho(0, "3000", 1);
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

        var client = NativeClient.initEcho(0, "3000", 1);
        BlockingRequest.createAccounts(client, null);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroCapacityBatch() {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new AccountBatch(0);

        BlockingRequest.createAccounts(client, batch);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroItemsBatch() {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new AccountBatch(1);

        BlockingRequest.createAccounts(client, batch);
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidOperation() throws RequestException {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);

        var dummyBuffer = ByteBuffer.allocateDirect(CreateTransferResultBatch.Struct.SIZE);

        assertFalse(request.isDone());

        // Invalid operation, should be CREATE_TRANSFERS
        request.endRequest(Request.Operations.LOOKUP_ACCOUNTS.value, dummyBuffer, 1,
                PacketStatus.Ok.value);

        assertTrue(request.isDone());

        request.waitForResult();
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithUnknownOperation() throws RequestException {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new TransferBatch(1);
        batch.add();

        final byte UNKNOWN = 99;
        var request = new BlockingRequest<CreateTransferResultBatch>(client,
                Operations.CREATE_TRANSFERS, batch);
        var dummyBuffer = ByteBuffer.allocateDirect(CreateTransferResultBatch.Struct.SIZE);

        assertFalse(request.isDone());

        // Unknown operation
        request.endRequest(UNKNOWN, dummyBuffer, 1, PacketStatus.Ok.value);

        assertTrue(request.isDone());

        request.waitForResult();
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithNullBuffer() throws RequestException {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);

        assertFalse(request.isDone());

        request.endRequest(Request.Operations.CREATE_TRANSFERS.value, null, 1,
                PacketStatus.Ok.value);

        assertTrue(request.isDone());

        request.waitForResult();
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidBufferSize() throws RequestException {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);
        var invalidBuffer =
                ByteBuffer.allocateDirect((CreateTransferResultBatch.Struct.SIZE * 2) - 1);

        assertFalse(request.isDone());

        request.endRequest(Request.Operations.CREATE_TRANSFERS.value, invalidBuffer, 1,
                PacketStatus.Ok.value);

        assertTrue(request.isDone());

        request.waitForResult();
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidPacket() throws RequestException {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new TransferBatch(1);
        batch.add();


        var request = BlockingRequest.createTransfers(client, batch);
        var dummyBuffer = ByteBuffer.allocateDirect(TransferBatch.Struct.SIZE);
        // Packet is a long representing a pointer, cannot be zero
        final long NULL = 0;

        assertFalse(request.isDone());
        request.endRequest(Request.Operations.CREATE_TRANSFERS.value, dummyBuffer, NULL,
                PacketStatus.Ok.value);

        assertTrue(request.isDone());

        request.waitForResult();
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testGetResultBeforeEndRequest() throws RequestException {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);

        assertFalse(request.isDone());

        request.getResult();
        assert false;
    }

    @Test
    public void testEndRequestWithRequestException() {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new TransferBatch(1);
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);

        assertFalse(request.isDone());

        var dummyBuffer = ByteBuffer.allocateDirect(CreateTransferResultBatch.Struct.SIZE);
        request.endRequest(Request.Operations.CREATE_TRANSFERS.value, dummyBuffer, 1,
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
    public void testEndRequestWithAmountOfResultsGreaterThanAmountOfRequests()
            throws RequestException {
        var client = NativeClient.initEcho(0, "3000", 1);

        // A batch with only 1 item
        var batch = new AccountBatch(1);
        batch.add();

        // A reply with 2 items, while the batch had only 1 item
        var dummyReplyBuffer = ByteBuffer.allocateDirect(CreateAccountResultBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);

        var request = BlockingRequest.createAccounts(client, batch);
        request.endRequest(Request.Operations.CREATE_ACCOUNTS.value, dummyReplyBuffer.position(0),
                1, PacketStatus.Ok.value);

        assertTrue(request.isDone());
        request.waitForResult();
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestTwice() throws RequestException {
        var client = NativeClient.initEcho(0, "3000", 1);

        // A batch with only 1 item
        var batch = new AccountBatch(1);
        batch.add();

        // A reply with 2 items, while the batch had only 1 item
        var dummyReplyBuffer = ByteBuffer.allocateDirect(CreateAccountResultBatch.Struct.SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);

        var request = BlockingRequest.createAccounts(client, batch);
        assertFalse(request.isDone());

        request.endRequest(Request.Operations.CREATE_ACCOUNTS.value, dummyReplyBuffer.position(0),
                1, PacketStatus.Ok.value);

        assertTrue(request.isDone());
        var result = request.waitForResult();
        assertEquals(1, result.getLength());

        request.endRequest(Request.Operations.CREATE_ACCOUNTS.value, dummyReplyBuffer.position(0),
                1, PacketStatus.Ok.value);

        assertTrue(request.isDone());

        request.waitForResult();
        assert false;
    }

    @Test
    public void testCreateAccountEndRequest() throws RequestException {
        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new AccountBatch(2);
        batch.add();
        batch.add();

        var request = BlockingRequest.createAccounts(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(CreateAccountResultBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putInt(0);
        dummyReplyBuffer.putInt(CreateAccountResult.IdMustNotBeZero.ordinal());
        dummyReplyBuffer.putInt(1);
        dummyReplyBuffer.putInt(CreateAccountResult.Exists.ordinal());

        request.endRequest(Request.Operations.CREATE_ACCOUNTS.value, dummyReplyBuffer.position(0),
                1, PacketStatus.Ok.value);

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
    public void testCreateTransferEndRequest() throws RequestException {
        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new TransferBatch(2);
        batch.add();
        batch.add();

        var request = BlockingRequest.createTransfers(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(CreateTransferResultBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putInt(0);
        dummyReplyBuffer.putInt(CreateTransferResult.IdMustNotBeZero.ordinal());
        dummyReplyBuffer.putInt(1);
        dummyReplyBuffer.putInt(CreateTransferResult.Exists.ordinal());

        request.endRequest(Request.Operations.CREATE_TRANSFERS.value, dummyReplyBuffer.position(0),
                1, PacketStatus.Ok.value);

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
    public void testLookupAccountEndRequest() throws RequestException {
        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new IdBatch(2);
        batch.add();
        batch.add();

        var request = BlockingRequest.lookupAccounts(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(AccountBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(AccountBatch.Struct.SIZE).putLong(200).putLong(2000);

        request.endRequest(Request.Operations.LOOKUP_ACCOUNTS.value, dummyReplyBuffer.position(0),
                1, PacketStatus.Ok.value);

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
    public void testLookupTransferEndRequest() throws RequestException {
        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new IdBatch(2);
        batch.add();
        batch.add();

        var request = BlockingRequest.lookupTransfers(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(TransferBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(TransferBatch.Struct.SIZE).putLong(200).putLong(2000);

        request.endRequest(Request.Operations.LOOKUP_TRANSFERS.value, dummyReplyBuffer.position(0),
                1, PacketStatus.Ok.value);

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
    public void testSuccessCompletion() throws RequestException {

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new IdBatch(2);
        batch.add();
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(TransferBatch.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);

        dummyReplyBuffer.putLong(100).putLong(1000);
        dummyReplyBuffer.position(TransferBatch.Struct.SIZE).putLong(200).putLong(2000);

        var callback =
                new CallbackSimulator<TransferBatch>(BlockingRequest.lookupTransfers(client, batch),
                        Request.Operations.LOOKUP_TRANSFERS.value, dummyReplyBuffer.position(0), 1,
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

        var client = NativeClient.initEcho(0, "3000", 1);
        var batch = new IdBatch(1);
        batch.add();

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(0);

        var callback =
                new CallbackSimulator<TransferBatch>(BlockingRequest.lookupTransfers(client, batch),
                        Request.Operations.LOOKUP_TRANSFERS.value, dummyReplyBuffer.position(0), 1,
                        PacketStatus.TooMuchData.value, 250);

        callback.start();

        try {
            callback.request.waitForResult();
            assert false;
        } catch (RequestException requestException) {

            assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());
        }

    }

    private class CallbackSimulator<T extends Batch> extends Thread {

        public final BlockingRequest<T> request;
        private final byte receivedOperation;
        private final ByteBuffer buffer;
        private final long packet;
        private final byte status;
        private final int delay;


        private CallbackSimulator(BlockingRequest<T> request, byte receivedOperation,
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
