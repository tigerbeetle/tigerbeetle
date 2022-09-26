package com.tigerbeetle;

import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.tigerbeetle.AssertionError.assertTrue;

public final class Client implements AutoCloseable {
    static {
        JNILoader.loadFromJar();
    }

    private static final int DEFAULT_MAX_CONCURRENCY = 32;

    private final int clusterID;
    private final int maxConcurrency;
    private final Semaphore maxConcurrencySemaphore;

    private final ReentrantLock packetsLock;
    private volatile long clientHandle;
    private volatile long packetsHead;
    private volatile long packetsTail;

    /**
     * Initializes an instance of TigerBeetle client. This class is thread-safe and for optimal
     * performance, a single instance should be shared between multiple concurrent tasks.
     * <p>
     * Multiple clients can be instantiated in case of connecting to more than one TigerBeetle
     * cluster.
     *
     * @param clusterID
     * @param replicaAddresses
     * @param maxConcurrency
     *
     * @throws InitializationException if an error occurred initializing this client. See
     *         {@link InitializationException.Status} for more details.
     *
     * @throws IllegalArgumentException if {@code clusterID} is negative.
     * @throws IllegalArgumentException if {@code replicaAddresses} is empty or presented in
     *         incorrect format.
     * @throws NullPointerException if {@code replicaAddresses} is null or any element in the array
     *         is null.
     * @throws IllegalArgumentException if {@code maxConcurrency} is zero or negative.
     */
    public Client(final int clusterID, final String[] replicaAddresses, final int maxConcurrency) {
        this(clusterID, maxConcurrency);

        if (replicaAddresses == null)
            throw new NullPointerException("Replica addresses cannot be null");

        if (replicaAddresses.length == 0)
            throw new IllegalArgumentException("Empty replica addresses");

        var joiner = new StringJoiner(",");
        for (var address : replicaAddresses) {
            if (address == null)
                throw new NullPointerException("Replica address cannot be null");
            joiner.add(address);
        }

        int status = clientInit(clusterID, joiner.toString(), maxConcurrency);

        if (status == InitializationException.Status.INVALID_ADDRESS)
            throw new IllegalArgumentException("Replica addresses format is invalid.");

        if (status != 0)
            throw new InitializationException(status);
    }


    /**
     * Initializes an instance of TigerBeetle client. This class is thread-safe and for optimal
     * performance, a single instance should be shared between multiple concurrent tasks.
     * <p>
     * Multiple clients can be instantiated in case of connecting to more than one TigerBeetle
     * cluster.
     *
     * @param clusterID
     * @param replicaAddresses
     *
     * @throws InitializationException if an error occurred initializing this client. See
     *         {@link InitializationException.Status} for more details.
     *
     * @throws IllegalArgumentException if {@code clusterID} is negative.
     * @throws IllegalArgumentException if {@code replicaAddresses} is empty or presented in
     *         incorrect format.
     * @throws NullPointerException if {@code replicaAddresses} is null or any element in the array
     *         is null.
     */
    public Client(final int clusterID, final String[] replicaAddresses) {
        this(clusterID, replicaAddresses, DEFAULT_MAX_CONCURRENCY);
    }

    Client(final int clusterID, final int maxConcurrency) {
        if (clusterID < 0)
            throw new IllegalArgumentException("ClusterID must be positive");

        // Cap the maximum amount of packets
        if (maxConcurrency <= 0)
            throw new IllegalArgumentException("Invalid maxConcurrency");

        if (maxConcurrency > 4096) {
            this.maxConcurrency = 4096;
        } else {
            this.maxConcurrency = maxConcurrency;
        }

        this.clusterID = clusterID;
        this.maxConcurrencySemaphore = new Semaphore(maxConcurrency, false);
        this.packetsLock = new ReentrantLock(false);
    }

    /**
     * Submits a batch of new accounts to be created.
     *
     * @param batch a {@link com.tigerbeetle.Accounts} instance containing all accounts to be
     *        created.
     * @return a {@link com.tigerbeetle.CreateAccountResults}
     * @throws RequestException refer to {@link com.tigerbeetle.RequestException.Status} for more
     *         details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CreateAccountResults createAccounts(final Accounts batch) throws RequestException {
        final var request = BlockingRequest.createAccounts(this, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Submits a batch of new accounts to be created asynchronously.
     *
     * @see Client#createAccounts(Accounts)
     * @param batch a {@link com.tigerbeetle.Accounts} instance containing all accounts to be
     *        created.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<CreateAccountResults> createAccountsAsync(final Accounts batch) {
        final var request = AsyncRequest.createAccounts(this, batch);
        request.beginRequest();
        return request.getFuture();
    }

    /**
     * Looks up a batch of accounts.
     *
     * @param batch an {@link com.tigerbeetle.Ids} containing all account ids.
     * @return a {@link com.tigerbeetle.Accounts} containing all accounts found.
     * @throws RequestException refer to {@link com.tigerbeetle.RequestException.Status} for more
     *         details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public Accounts lookupAccounts(final Ids batch) throws RequestException {
        final var request = BlockingRequest.lookupAccounts(this, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Looks up a batch of accounts asynchronously.
     *
     * @see Client#lookupAccounts
     * @param batch an {@link com.tigerbeetle.Ids} containing all account ids.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<Accounts> lookupAccountsAsync(final Ids batch) {
        final var request = AsyncRequest.lookupAccounts(this, batch);
        request.beginRequest();
        return request.getFuture();
    }

    /**
     * Submits a batch of new transfers to be created.
     *
     * @param batch a {@link com.tigerbeetle.Transfers} instance containing all transfers to be
     *        created.
     * @return a {@link com.tigerbeetle.CreateTransferResults} describing the reason.
     * @throws RequestException refer to {@link com.tigerbeetle.RequestException.Status} for more
     *         details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CreateTransferResults createTransfers(final Transfers batch) throws RequestException {
        final var request = BlockingRequest.createTransfers(this, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Submits a batch of new transfers to be created asynchronously.
     *
     * @param batch a {@link com.tigerbeetle.Transfers} instance containing all transfers to be
     *        created.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<CreateTransferResults> createTransfersAsync(final Transfers batch) {
        final var request = AsyncRequest.createTransfers(this, batch);
        request.beginRequest();
        return request.getFuture();
    }


    /**
     * Looks up a batch of transfers.
     *
     * @param batch an {@link com.tigerbeetle.Ids} containing all transfer ids.
     * @return a {@link com.tigerbeetle.Transfers} containing all transfers found.
     * @throws RequestException refer to {@link com.tigerbeetle.RequestException.Status} for more
     *         details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public Transfers lookupTransfers(final Ids batch) throws RequestException {
        final var request = BlockingRequest.lookupTransfers(this, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Looks up a batch of transfers asynchronously.
     *
     * @see Client#lookupTransfers(Ids)
     * @param batch an {@link com.tigerbeetle.Ids} containing all transfer ids.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<Transfers> lookupTransfersAsync(final Ids batch) {
        final var request = AsyncRequest.lookupTransfers(this, batch);
        request.beginRequest();
        return request.getFuture();
    }

    void submit(final Request<?> request) {
        final long packet = adquirePacket();
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

        packetsLock.lock();
        try {
            return popPacket(packetsHead, packetsTail);
        } finally {
            packetsLock.unlock();
        }
    }

    void returnPacket(final long packet) {

        // It is not expected to return a packet with a disconnected client,
        // since we wait for all pending requests before zeroing the handle.
        // This condition allows running tests without initializing the client

        if (clientHandle != 0) {

            assertTrue(packet != 0L, "Packet cannot be null.");

            packetsLock.lock();
            try {
                pushPacket(packetsHead, packetsTail, packet);
            } finally {
                packetsLock.unlock();
            }
        }

        // Releasing the packet to be used by another thread
        maxConcurrencySemaphore.release();
    }

    /*
     * Closes the client, freeing all resources. <p> This method causes the current thread to wait
     * for all ongoing requests to finish.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() throws Exception {

        if (clientHandle != 0) {

            // Acquire all permits, forcing to wait for any processing thread to release
            this.maxConcurrencySemaphore.acquireUninterruptibly(maxConcurrency);

            // Deinit and sinalize that this client is closed by setting the handles to 0
            packetsLock.lock();
            try {

                clientDeinit(clientHandle);

                clientHandle = 0;
                packetsHead = 0;
                packetsTail = 0;
            } finally {
                packetsLock.unlock();
            }
        }
    }

    private native void submit(long clientHandle, Request<?> request, long packet);

    private native int clientInit(int clusterID, String addresses, int maxConcurrency);

    private native void clientDeinit(long clientHandle);

    private native long popPacket(long packetHead, long packetTail);

    private native void pushPacket(long packetHead, long packetTail, long packet);
}
