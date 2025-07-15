package com.tigerbeetle;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;

public final class Client implements AutoCloseable {

    private final byte[] clusterID;
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
     *
     * @throws InitializationException if an error occurred initializing this client. See
     *         {@link InitializationStatus} for more details.
     *
     * @throws NullPointerException if {@code clusterID} is null.
     * @throws IllegalArgumentException if {@code clusterID} is not a UInt128.
     * @throws IllegalArgumentException if {@code replicaAddresses} is empty or presented in
     *         incorrect format.
     * @throws NullPointerException if {@code replicaAddresses} is null or any element in the array
     *         is null.
     */
    public Client(final byte[] clusterID, final String[] replicaAddresses) {
        Objects.requireNonNull(clusterID, "ClusterID cannot be null");
        if (clusterID.length != UInt128.SIZE)
            throw new IllegalArgumentException("ClusterID must be 16 bytes long");

        Objects.requireNonNull(replicaAddresses, "Replica addresses cannot be null");

        if (replicaAddresses.length == 0)
            throw new IllegalArgumentException("Empty replica addresses");

        var joiner = new StringJoiner(",");
        for (var address : replicaAddresses) {
            Objects.requireNonNull(address, "Replica address cannot be null");
            joiner.add(address);
        }

        this.clusterID = clusterID;
        this.nativeClient = NativeClient.init(clusterID, joiner.toString());
    }

    /**
     * Gets the cluster ID
     *
     * @return clusterID
     */
    public byte[] getClusterID() {
        return clusterID;
    }

    /**
     * Submits a batch of new accounts to be created.
     *
     * @param batch a {@link com.tigerbeetle.AccountBatch batch} containing all accounts to be
     *        created.
     * @return a read-only {@link com.tigerbeetle.CreateAccountResultBatch batch} describing the
     *         result.
     * @throws RequestException refer to {@link PacketStatus} for more details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CreateAccountResultBatch createAccounts(final AccountBatch batch) {
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
     * @throws RequestException refer to {@link PacketStatus} for more details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public AccountBatch lookupAccounts(final IdBatch batch) {
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
     * @throws RequestException refer to {@link PacketStatus} for more details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CreateTransferResultBatch createTransfers(final TransferBatch batch) {
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
     * Submits a batch of new transfers to be created and returns the outcome.
     *
     * @param batch a {@link com.tigerbeetle.TransferBatch batch} containing all transfers to be
     *        created.
     * @return a read-only {@link com.tigerbeetle.CreateAndReturnTransferResultBatch batch}
     *         containing information about the outcome.
     * @throws RequestException refer to {@link PacketStatus} for more details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CreateAndReturnTransferResultBatch createAndReturnTransfers(final TransferBatch batch) {
        final var request = BlockingRequest.createAndReturnTransfers(this.nativeClient, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Submits a batch of new transfers to be created asynchronously and returns the outcome.
     *
     * @param batch a {@link com.tigerbeetle.TransferBatch batch} containing all transfers to be
     *        created.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<CreateAndReturnTransferResultBatch> createAndReturnTransfersAsync(
            final TransferBatch batch) {
        final var request = AsyncRequest.createAndReturnTransfers(this.nativeClient, batch);
        request.beginRequest();
        return request.getFuture();
    }

    /**
     * Looks up a batch of transfers.
     *
     * @param batch a {@link com.tigerbeetle.IdBatch batch} containing all transfer ids.
     * @return a read-only {@link com.tigerbeetle.TransferBatch batch} containing all transfers
     *         found.
     * @throws RequestException refer to {@link PacketStatus} for more details.
     * @throws IllegalArgumentException if {@code batch} is empty.
     * @throws NullPointerException if {@code batch} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public TransferBatch lookupTransfers(final IdBatch batch) {
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
     * Fetch transfers from a given account.
     *
     * @see Client#getAccountTransfers(AccountFilter)
     * @param filter a {@link com.tigerbeetle.AccountFilter} containing all query parameters.
     * @return a read-only {@link com.tigerbeetle.TransferBatch batch} containing all transfers that
     *         match the query parameters.
     * @throws NullPointerException if {@code filter} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public TransferBatch getAccountTransfers(final AccountFilter filter) {
        final var request = BlockingRequest.getAccountTransfers(this.nativeClient, filter);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Fetch transfers from a given account asynchronously.
     *
     * @see Client#getAccountTransfers(AccountFilter)
     * @param filter a {@link com.tigerbeetle.AccountFilter} containing all query parameters.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws NullPointerException if {@code filter} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<TransferBatch> getAccountTransfersAsync(final AccountFilter filter) {
        final var request = AsyncRequest.getAccountTransfers(this.nativeClient, filter);
        request.beginRequest();
        return request.getFuture();
    }

    /**
     * Fetch the balance history from a given account.
     *
     * @see Client#getAccountBalances(AccountFilter)
     * @param filter a {@link com.tigerbeetle.AccountFilter} containing all query parameters.
     * @return a read-only {@link com.tigerbeetle.AccountBalanceBatch batch} containing all balances
     *         that match the query parameters.
     * @throws NullPointerException if {@code filter} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public AccountBalanceBatch getAccountBalances(final AccountFilter filter) {
        final var request = BlockingRequest.getAccountBalances(this.nativeClient, filter);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Fetch the balance history from a given account asynchronously.
     *
     * @see Client#getAccountBalances(AccountFilter)
     * @param filter a {@link com.tigerbeetle.AccountFilter} containing all query parameters.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws NullPointerException if {@code filter} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<AccountBalanceBatch> getAccountBalancesAsync(
            final AccountFilter filter) {
        final var request = AsyncRequest.getAccountBalances(this.nativeClient, filter);
        request.beginRequest();
        return request.getFuture();
    }

    /**
     * Query accounts.
     *
     * @param filter a {@link com.tigerbeetle.QueryFilter} containing all query parameters.
     * @return a read-only {@link com.tigerbeetle.AccountBatch batch} containing all accounts that
     *         match the query parameters.
     * @throws NullPointerException if {@code filter} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public AccountBatch queryAccounts(final QueryFilter filter) {
        final var request = BlockingRequest.queryAccounts(this.nativeClient, filter);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Query accounts asynchronously.
     *
     * @see Client#queryAccounts(QueryFilter)
     * @param filter a {@link com.tigerbeetle.QueryFilter} containing all query parameters.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws NullPointerException if {@code filter} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<AccountBatch> queryAccountsAsync(final QueryFilter filter) {
        final var request = AsyncRequest.queryAccounts(this.nativeClient, filter);
        request.beginRequest();
        return request.getFuture();
    }

    /**
     * Query transfers.
     *
     * @param filter a {@link com.tigerbeetle.QueryFilter} containing all query parameters.
     * @return a read-only {@link com.tigerbeetle.TransferBatch batch} containing all transfers that
     *         match the query parameters.
     * @throws NullPointerException if {@code filter} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public TransferBatch queryTransfers(final QueryFilter filter) {
        final var request = BlockingRequest.queryTransfers(this.nativeClient, filter);
        request.beginRequest();
        return request.waitForResult();
    }

    /**
     * Query transfers asynchronously.
     *
     * @see Client#queryTransfers(QueryFilter)
     * @param filter a {@link com.tigerbeetle.QueryFilter} containing all query parameters.
     * @return a {@link java.util.concurrent.CompletableFuture} to be completed.
     * @throws NullPointerException if {@code filter} is null.
     * @throws IllegalStateException if this client is closed.
     */
    public CompletableFuture<TransferBatch> queryTransfersAsync(final QueryFilter filter) {
        final var request = AsyncRequest.queryTransfers(this.nativeClient, filter);
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
    public void close() {
        nativeClient.close();
    }
}
