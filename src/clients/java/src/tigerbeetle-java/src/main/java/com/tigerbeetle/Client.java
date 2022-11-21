package com.tigerbeetle;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class Client implements AutoCloseable {
    static {
        JNILoader.loadFromJar();
    }

    private static final int DEFAULT_MAX_CONCURRENCY = 32;

    private final int clusterID;
    private final int maxConcurrency;
    private final Semaphore maxConcurrencySemaphore;
    private volatile long contextHandle;

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
        if (clusterID < 0)
            throw new IllegalArgumentException("ClusterID must be positive");

        if (maxConcurrency <= 0)
            throw new IllegalArgumentException("Invalid maxConcurrency");

        Objects.requireNonNull(replicaAddresses, "Replica addresses cannot be null");

        if (replicaAddresses.length == 0)
            throw new IllegalArgumentException("Empty replica addresses");

        var joiner = new StringJoiner(",");
        for (var address : replicaAddresses) {
            Objects.requireNonNull(address, "Replica address cannot be null");
            joiner.add(address);
        }

        int status = clientInit(clusterID, joiner.toString(), maxConcurrency);

        switch (status) {
            case InitializationException.Status.SUCCESS:
                this.clusterID = clusterID;
                this.maxConcurrency = maxConcurrency;
                this.maxConcurrencySemaphore = new Semaphore(maxConcurrency, false);
                break;

            case InitializationException.Status.ADDRESS_INVALID:
                throw new IllegalArgumentException("Replica addresses format is invalid");

            case InitializationException.Status.ADDRESS_LIMIT_EXCEEDED:
                throw new IllegalArgumentException("Replica addresses limit exceeded");

            case InitializationException.Status.PACKETS_COUNT_INVALID:
                throw new IllegalArgumentException("Invalid maxConcurrency");

            default:
                throw new InitializationException(status);
        }
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
        if (maxConcurrency <= 0)
            throw new IllegalArgumentException("Invalid maxConcurrency");

        this.clusterID = clusterID;
        this.maxConcurrency = maxConcurrency;
        this.maxConcurrencySemaphore = new Semaphore(maxConcurrency, false);
    }

    /**
     * Submits a batch of new accounts to be created.
     *
     * @param batch a {@link com.tigerbeetle.AccountBatch batch} containing all accounts to be
     *        created.
     * @return a read-only {@link com.tigerbeetle.CreateAccountResultBatch batch} describing the
     *         result.
     * @throws RequestException refer to {@link com.tigerbeetle.RequestException.Status} for more
     *         details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CreateAccountResultBatch createAccounts(final AccountBatch batch)
            throws RequestException {
        final var request = BlockingRequest.createAccounts(this, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Submits a batch of new accounts to be created asynchronously.
     *
     * @see Client#createAccounts(AccountBatch)
     * @param batch a {@link com.tigerbeetle.AccountBatch batch} containing all accounts to be
     *        created.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<CreateAccountResultBatch> createAccountsAsync(
            final AccountBatch batch) {
        final var request = AsyncRequest.createAccounts(this, batch);
        request.beginRequest();
        return request.getFuture();
    }

    /**
     * Looks up a batch of accounts.
     *
     * @param batch an {@link com.tigerbeetle.IdBatch batch} containing all account ids.
     * @return a read-only {@link com.tigerbeetle.AccountBatch batch} containing all accounts found.
     * @throws RequestException refer to {@link com.tigerbeetle.RequestException.Status} for more
     *         details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public AccountBatch lookupAccounts(final IdBatch batch) throws RequestException {
        final var request = BlockingRequest.lookupAccounts(this, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Looks up a batch of accounts asynchronously.
     *
     * @see Client#lookupAccounts
     * @param batch a {@link com.tigerbeetle.IdBatch batch} containing all account ids.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<AccountBatch> lookupAccountsAsync(final IdBatch batch) {
        final var request = AsyncRequest.lookupAccounts(this, batch);
        request.beginRequest();
        return request.getFuture();
    }

    /**
     * Submits a batch of new transfers to be created.
     *
     * @param batch a {@link com.tigerbeetle.TransferBatch batch} containing all transfers to be
     *        created.
     * @return a read-only {@link com.tigerbeetle.CreateTransferResultBatch batch} describing the
     *         result.
     * @throws RequestException refer to {@link com.tigerbeetle.RequestException.Status} for more
     *         details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CreateTransferResultBatch createTransfers(final TransferBatch batch)
            throws RequestException {
        final var request = BlockingRequest.createTransfers(this, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Submits a batch of new transfers to be created asynchronously.
     *
     * @param batch a {@link com.tigerbeetle.TransferBatch batch} containing all transfers to be
     *        created.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<CreateTransferResultBatch> createTransfersAsync(
            final TransferBatch batch) {
        final var request = AsyncRequest.createTransfers(this, batch);
        request.beginRequest();
        return request.getFuture();
    }

    /**
     * Looks up a batch of transfers.
     *
     * @param batch a {@link com.tigerbeetle.IdBatch batch} containing all transfer ids.
     * @return a read-only {@link com.tigerbeetle.TransferBatch batch} containing all transfers
     *         found.
     * @throws RequestException refer to {@link com.tigerbeetle.RequestException.Status} for more
     *         details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public TransferBatch lookupTransfers(final IdBatch batch) throws RequestException {
        final var request = BlockingRequest.lookupTransfers(this, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Looks up a batch of transfers asynchronously.
     *
     * @see Client#lookupTransfers(IdBatch)
     * @param batch a {@link com.tigerbeetle.IdBatch batch} containing all transfer ids.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<TransferBatch> lookupTransfersAsync(final IdBatch batch) {
        final var request = AsyncRequest.lookupTransfers(this, batch);
        request.beginRequest();
        return request.getFuture();
    }

    void submit(final Request<?> request) {
        acquirePermit();
        submit(contextHandle, request);
    }

    private void acquirePermit() {

        // Assure that only the max number of concurrent requests can acquire a packet
        // It forces other threads to wait until a packet became available
        // We also assure that the clientHandle will be zeroed only after all permits
        // have been released
        final int TIMEOUT = 5;
        boolean acquired = false;
        do {

            if (contextHandle == 0)
                throw new IllegalStateException("Client is closed");

            try {
                acquired = maxConcurrencySemaphore.tryAcquire(TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException interruptedException) {

                // This exception should never exposed by the API to be handled by the user
                throw new AssertionError(interruptedException,
                        "Unexpected thread interruption on acquiring a packet.");
            }

        } while (!acquired);
    }

    void releasePermit() {
        // Releasing the packet to be used by another thread
        maxConcurrencySemaphore.release();
    }

    /**
     * Closes the client, freeing all resources.
     * <p>
     * This method causes the current thread to wait for all ongoing requests to finish.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() throws Exception {

        if (contextHandle != 0) {

            // Acquire all permits, forcing to wait for any processing thread to release
            // Note that "acquireUninterruptibly(maxConcurrency)" atomically acquires the
            // permits
            // all at once, causing this operation to take longer.
            for (int i = 0; i < maxConcurrency; i++) {
                this.maxConcurrencySemaphore.acquireUninterruptibly();
            }

            // Deinit and signalize that this client is closed by setting the handles to 0
            clientDeinit(contextHandle);
            contextHandle = 0;
        }
    }

    private native void submit(long contextHandle, Request<?> request);

    private native int clientInit(int clusterID, String addresses, int maxConcurrency);

    private native void clientDeinit(long contextHandle);
}
