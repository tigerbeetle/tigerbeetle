package com.tigerbeetle;

import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class Client implements AutoCloseable {
    static {
        try {
            // Tries to load the library bundled into the jar
            JNILoader.loadFromJar();
        } catch (Throwable any) {
            // Fallback to the JVM Path
            System.loadLibrary(JNILoader.libName);
        }
    }

    private static final int DEFAULT_MAX_CONCURRENCY = 32;

    private final int clusterID;
    private final int maxConcurrency;
    private final Semaphore maxConcurrencySemaphore;
    private long clientHandle;
    private long packetsHead;
    private long packetsTail;

    public Client(int clusterID, String[] replicaAddresses) {
        this(clusterID, replicaAddresses, DEFAULT_MAX_CONCURRENCY);
    }

    public Client(int clusterID, String[] replicaAddresses, int maxConcurrency) {
        if (clusterID < 0)
            throw new IllegalArgumentException("ClusterID must be positive");

        if (replicaAddresses == null || replicaAddresses.length == 0)
            throw new IllegalArgumentException("Invalid replica addresses");

        // Cap the maximum amount of packets
        if (maxConcurrency <= 0)
            throw new IllegalArgumentException("Invalid maxConcurrency");

        if (maxConcurrency > 4096) {
            maxConcurrency = 4096;
        }

        var joiner = new StringJoiner(",");
        for (var address : replicaAddresses) {
            joiner.add(address);
        }

        this.clusterID = clusterID;
        int status = clientInit(clusterID, joiner.toString(), maxConcurrency);

        if (status == InitializationException.Status.INVALID_ADDRESS)
            throw new IllegalArgumentException("Replica addresses format is invalid.");

        if (status != 0)
            throw new InitializationException(status);

        this.maxConcurrency = maxConcurrency;
        this.maxConcurrencySemaphore = new Semaphore(maxConcurrency, false);
    }

    public CreateAccountResult createAccount(Account account) throws RequestException {
        var batch = new AccountsBatch(1);
        batch.add(account);

        CreateAccountsResult[] results = createAccounts(batch);
        if (results.length == 0) {
            return CreateAccountResult.Ok;
        } else {
            return results[0].result;
        }
    }

    public CreateAccountsResult[] createAccounts(Account[] batch) throws RequestException {
        return createAccounts(new AccountsBatch(batch));
    }

    public CreateAccountsResult[] createAccounts(AccountsBatch batch) throws RequestException {
        var request = new CreateAccountsRequest(this, batch);
        request.beginRequest();
        request.waitForCompletionUninterruptibly();
        return request.getResult();
    }

    public Future<CreateAccountsResult[]> createAccountsAsync(Account[] batch) {
        return createAccountsAsync(new AccountsBatch(batch));
    }

    public Future<CreateAccountsResult[]> createAccountsAsync(AccountsBatch batch) {
        var request = new CreateAccountsRequest(this, batch);
        request.beginRequest();
        return request;
    }

    public Account lookupAccount(UUID uuid) throws RequestException {
        var batch = new UUIDsBatch(1);
        batch.Add(uuid);

        Account[] results = lookupAccounts(batch);
        if (results.length == 0) {
            return null;
        } else {
            return results[0];
        }
    }

    public Account[] lookupAccounts(UUID[] batch) throws RequestException {
        return lookupAccounts(new UUIDsBatch(batch));
    }

    public Account[] lookupAccounts(UUIDsBatch batch) throws RequestException {
        var request = new LookupAccountsRequest(this, batch);
        request.beginRequest();
        request.waitForCompletionUninterruptibly();
        return request.getResult();
    }

    public Future<Account[]> lookupAccountsAsync(UUID[] batch) {
        return lookupAccountsAsync(new UUIDsBatch(batch));
    }

    public Future<Account[]> lookupAccountsAsync(UUIDsBatch batch) {
        var request = new LookupAccountsRequest(this, batch);
        request.beginRequest();
        return request;
    }

    public CreateTransferResult createTransfer(Transfer transfer) throws RequestException {
        var batch = new TransfersBatch(1);
        batch.add(transfer);

        CreateTransfersResult[] results = createTransfers(batch);
        if (results.length == 0) {
            return CreateTransferResult.Ok;
        } else {
            return results[0].result;
        }
    }

    public CreateTransfersResult[] createTransfers(Transfer[] batch) throws RequestException {
        return createTransfers(new TransfersBatch(batch));
    }

    public CreateTransfersResult[] createTransfers(TransfersBatch batch) throws RequestException {
        var request = new CreateTransfersRequest(this, batch);
        request.beginRequest();
        request.waitForCompletionUninterruptibly();
        return request.getResult();
    }

    public Future<CreateTransfersResult[]> createTransfersAsync(Transfer[] batch) {
        return createTransfersAsync(new TransfersBatch(batch));
    }

    public Future<CreateTransfersResult[]> createTransfersAsync(TransfersBatch batch) {
        var request = new CreateTransfersRequest(this, batch);
        request.beginRequest();
        return request;
    }

    public Transfer lookupTransfer(UUID uuid) throws RequestException {
        var batch = new UUIDsBatch(1);
        batch.Add(uuid);

        Transfer[] results = lookupTransfers(batch);
        if (results.length == 0) {
            return null;
        } else {
            return results[0];
        }
    }

    public Transfer[] lookupTransfers(UUID[] batch) throws RequestException {
        return lookupTransfers(new UUIDsBatch(batch));
    }

    public Transfer[] lookupTransfers(UUIDsBatch batch) throws RequestException {
        var request = new LookupTransfersRequest(this, batch);
        request.beginRequest();
        request.waitForCompletionUninterruptibly();
        return request.getResult();
    }

    public Future<Transfer[]> lookupTransfersAsync(UUID[] batch) {
        return lookupTransfersAsync(new UUIDsBatch(batch));
    }

    public Future<Transfer[]> lookupTransfersAsync(UUIDsBatch batch) {
        var request = new LookupTransfersRequest(this, batch);
        request.beginRequest();
        return request;
    }

    void submit(Request<?> request) {
        long packet = adquirePacket();
        submit(clientHandle, request, packet);
    }

    private long adquirePacket() {

        // Assure that only the max number of concurrent requests can adquire a packet
        // It forces other threads to wait until a packet became available
        // We also assure that the clientHandle will be zeroed only after all permits
        // have been released
        final int TIMEOUT = 5;
        boolean adquired = false;
        do {

            if (clientHandle == 0)
                throw new IllegalStateException("Client is closed");

            try {
                adquired = maxConcurrencySemaphore.tryAcquire(TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException interruptedException) {

                // This exception should never exposed by the API to be handled by the user
                throw new AssertionError(interruptedException,
                        "Unexpected thread interruption on adquiring a packet.");
            }

        } while (!adquired);

        synchronized (this) {
            return popPacket(packetsHead, packetsTail);
        }
    }

    void returnPacket(long packet) {
        synchronized (this) {
            // Check if the client is closing
            if (clientHandle != 0) {
                pushPacket(packetsHead, packetsTail, packet);
            }
        }

        // Releasing the packet to be used by another thread
        maxConcurrencySemaphore.release();
    }

    @Override
    public void close() throws Exception {

        if (clientHandle != 0) {

            // Acquire all permits, forcing to wait for any processing thread to release
            this.maxConcurrencySemaphore.acquireUninterruptibly(maxConcurrency);

            // Deinit and sinalize that this client is closed by setting the handles to 0
            synchronized (this) {
                clientDeinit(clientHandle);

                clientHandle = 0;
                packetsHead = 0;
                packetsTail = 0;
            }
        }
    }

    private native void submit(long clientHandle, Request<?> request, long packet);

    private native int clientInit(int clusterID, String addresses, int maxConcurrency);

    private native void clientDeinit(long clientHandle);

    private native long popPacket(long packetHead, long packetTail);

    private native void pushPacket(long packetHead, long packetTail, long packet);
}
