package com.tigerbeetle;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;

public final class Client implements AutoCloseable {

    private static final int DEFAULT_MAX_CONCURRENCY = 32;

    private final int clusterID;
    private final NativeClient nativeClient;

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

        this.clusterID = clusterID;
        this.nativeClient = NativeClient.init(clusterID, joiner.toString(), maxConcurrency);
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

    /**
     * Gets the cluster ID
     *
     * @return clusterID
     */
    public int getClusterID() {
        return clusterID;
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
        final var request = BlockingRequest.createAccounts(this.nativeClient, batch);
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
        final var request = AsyncRequest.createAccounts(this.nativeClient, batch);
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
        final var request = BlockingRequest.lookupAccounts(this.nativeClient, batch);
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
        final var request = AsyncRequest.lookupAccounts(this.nativeClient, batch);
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
        final var request = BlockingRequest.createTransfers(this.nativeClient, batch);
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
        final var request = AsyncRequest.createTransfers(this.nativeClient, batch);
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
        final var request = BlockingRequest.lookupTransfers(this.nativeClient, batch);
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
        final var request = AsyncRequest.lookupTransfers(this.nativeClient, batch);
        request.beginRequest();
        return request.getFuture();
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
        nativeClient.close();
    }
}
