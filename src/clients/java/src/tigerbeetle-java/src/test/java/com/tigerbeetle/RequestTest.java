package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import org.junit.Test;

public class RequestTest {

    @Test
    public void testCreateAccountsRequestConstructor() {

        var client = new Client(0, 1);
        var batch = new AccountsBatch(1);
        batch.add(new Account());

        var request = new CreateAccountsRequest(client, batch);
        assert request != null;
    }

    @Test
    public void testCreateTransfersRequestConstructor() {

        var client = new Client(0, 1);
        var batch = new TransfersBatch(1);
        batch.add(new Transfer());

        var request = new CreateTransfersRequest(client, batch);
        assert request != null;
    }

    @Test
    public void testLookupAccountsRequestConstructor() {

        var client = new Client(0, 1);
        var batch = new UUIDsBatch(1);
        batch.add(new UUID(0, 0));

        var request = new LookupAccountsRequest(client, batch);
        assert request != null;
    }

    @Test
    public void testLookupTransfersRequestConstructor() {

        var client = new Client(0, 1);
        var batch = new UUIDsBatch(1);
        batch.add(new UUID(0, 0));

        var request = new LookupTransfersRequest(client, batch);
        assert request != null;
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithClientNull() {

        var batch = new AccountsBatch(1);
        batch.add(new Account());

        new CreateAccountsRequest(null, batch);
        assert false;
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithBatchNull() {

        var client = new Client(0, 1);
        new CreateAccountsRequest(client, null);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroCapacityBatch() {

        var client = new Client(0, 1);
        var batch = new AccountsBatch(0);

        new CreateAccountsRequest(client, batch);
        assert false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroItemsBatch() {

        var client = new Client(0, 1);
        var batch = new AccountsBatch(1);

        new CreateAccountsRequest(client, batch);
        assert false;
    }

    @Test
    public void testItIsNotCancellable() {

        var client = new Client(0, 1);
        var batch = new TransfersBatch(1);
        batch.add(new Transfer());

        var request = new CreateTransfersRequest(client, batch);

        assertFalse(request.cancel(true));
        assertFalse(request.cancel(false));
        assertFalse(request.isCancelled());
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidOperation() throws RequestException {

        var client = new Client(0, 1);
        var batch = new TransfersBatch(1);
        batch.add(new Transfer());

        var request = new CreateTransfersRequest(client, batch);

        var dummyBuffer = ByteBuffer.allocateDirect(CreateTransfersResult.Struct.SIZE);

        // Invalid operation, should be CREATE_TRANSFERS
        request.endRequest(Request.Operations.LOOKUP_ACCOUNTS, dummyBuffer, 1,
                RequestException.Status.OK);
        request.getResult();
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithNullBuffer() throws RequestException {

        var client = new Client(0, 1);
        var batch = new TransfersBatch(1);
        batch.add(new Transfer());

        var request = new CreateTransfersRequest(client, batch);
        request.endRequest(Request.Operations.CREATE_TRANSFERS, null, 1,
                RequestException.Status.OK);
        request.getResult();
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidBufferSize() throws RequestException {

        var client = new Client(0, 1);
        var batch = new TransfersBatch(1);
        batch.add(new Transfer());

        var request = new CreateTransfersRequest(client, batch);
        var invalidBuffer = ByteBuffer.allocateDirect((CreateTransfersResult.Struct.SIZE * 2) - 1);
        request.endRequest(Request.Operations.CREATE_TRANSFERS, invalidBuffer, 1,
                RequestException.Status.OK);
        request.getResult();
        assert false;
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithInvalidPacket() throws RequestException {

        var client = new Client(0, 1);
        var batch = new TransfersBatch(1);
        batch.add(new Transfer());


        var request = new CreateTransfersRequest(client, batch);
        var dummyBuffer = ByteBuffer.allocateDirect(Transfer.Struct.SIZE);
        // Packet is a long representing a pointer, cannot be zero
        final long NULL = 0;

        request.endRequest(Request.Operations.CREATE_TRANSFERS, dummyBuffer, NULL,
                RequestException.Status.OK);
        request.getResult();
        assert false;
    }

    @Test
    public void testEndRequestWithResultException() {

        var client = new Client(0, 1);
        var batch = new TransfersBatch(1);
        batch.add(new Transfer());

        var request = new CreateTransfersRequest(client, batch);
        var dummyBuffer = ByteBuffer.allocateDirect(CreateTransfersResult.Struct.SIZE);
        request.endRequest(Request.Operations.CREATE_TRANSFERS, dummyBuffer, 1,
                RequestException.Status.TOO_MUCH_DATA);

        try {
            request.getResult();
            assert false;
        } catch (RequestException e) {
            assertEquals(RequestException.Status.TOO_MUCH_DATA, e.getStatus());
        }
    }

    @Test(expected = AssertionError.class)
    public void testEndRequestWithAmountOfResultsGreaterThanAmountOfRequests()
            throws RequestException {
        var client = new Client(0, 1);

        // A batch with only 1 item
        var batch = new AccountsBatch(1);
        batch.add(new Account());

        // A reply with 2 items, while the batch had only 1 item
        var dummyReplyBuffer = ByteBuffer.allocateDirect(CreateAccountsResult.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);

        var request = new CreateAccountsRequest(client, batch);
        request.endRequest(Request.Operations.CREATE_ACCOUNTS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.OK);

        request.getResult();
    }

    @Test
    public void testCreateAccountEndRequest() throws RequestException {
        var client = new Client(0, 1);
        var batch = new AccountsBatch(2);
        batch.add(new Account());
        batch.add(new Account());

        var request = new CreateAccountsRequest(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(CreateAccountsResult.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putInt(0);
        dummyReplyBuffer.putInt(CreateAccountResult.IdMustNotBeZero.ordinal());
        dummyReplyBuffer.putInt(1);
        dummyReplyBuffer.putInt(CreateAccountResult.Exists.ordinal());

        request.endRequest(Request.Operations.CREATE_ACCOUNTS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.OK);

        var result = request.getResult();
        assertEquals(2, result.length);

        assertEquals(0, result[0].index);
        assertEquals(CreateAccountResult.IdMustNotBeZero, result[0].result);

        assertEquals(1, result[1].index);
        assertEquals(CreateAccountResult.Exists, result[1].result);
    }

    @Test
    public void testCreateTransferEndRequest() throws RequestException {
        var client = new Client(0, 1);
        var batch = new TransfersBatch(2);
        batch.add(new Transfer());
        batch.add(new Transfer());

        var request = new CreateTransfersRequest(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer = ByteBuffer.allocateDirect(CreateTransfersResult.Struct.SIZE * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putInt(0);
        dummyReplyBuffer.putInt(CreateTransferResult.IdMustNotBeZero.ordinal());
        dummyReplyBuffer.putInt(1);
        dummyReplyBuffer.putInt(CreateTransferResult.Exists.ordinal());

        request.endRequest(Request.Operations.CREATE_TRANSFERS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.OK);

        var result = request.getResult();
        assertEquals(2, result.length);

        assertEquals(0, result[0].index);
        assertEquals(CreateTransferResult.IdMustNotBeZero, result[0].result);

        assertEquals(1, result[1].index);
        assertEquals(CreateTransferResult.Exists, result[1].result);
    }

    @Test
    public void testLookupAccountEndRequest() throws RequestException {
        var client = new Client(0, 1);
        var batch = new UUIDsBatch(2);
        batch.add(new UUID(0, 1));
        batch.add(new UUID(0, 2));

        var request = new LookupAccountsRequest(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocateDirect(Account.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(1);
        dummyReplyBuffer.position(Account.Struct.SIZE).putLong(2);

        request.endRequest(Request.Operations.LOOKUP_ACCOUNTS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.OK);

        var result = request.getResult();
        assertEquals(2, result.length);

        assertEquals(new UUID(0, 1), result[0].getId());
        assertEquals(new UUID(0, 2), result[1].getId());
    }

    @Test
    public void testLookupTransferEndRequest() throws RequestException {
        var client = new Client(0, 1);
        var batch = new UUIDsBatch(2);
        batch.add(new UUID(0, 1));
        batch.add(new UUID(0, 2));

        var request = new LookupTransfersRequest(client, batch);

        // A dummy ByteBuffer simulating some simple reply
        var dummyReplyBuffer =
                ByteBuffer.allocateDirect(Transfer.Struct.SIZE * 2).order(ByteOrder.LITTLE_ENDIAN);
        dummyReplyBuffer.putLong(1);
        dummyReplyBuffer.position(Transfer.Struct.SIZE).putLong(2);

        request.endRequest(Request.Operations.LOOKUP_TRANSFERS, dummyReplyBuffer.position(0), 1,
                RequestException.Status.OK);

        var result = request.getResult();
        assertEquals(2, result.length);

        assertEquals(new UUID(0, 1), result[0].getId());
        assertEquals(new UUID(0, 2), result[1].getId());
    }
}
