package com.tigerbeetle;

import java.util.concurrent.CompletableFuture;

final class AsyncRequest<TResponse extends Batch> extends Request<TResponse> {

    // @formatter:off
    /*
     * Overview:
     *
     * Implements a Request to be used when invoked asynchronously.
     * Exposes a CompletableFuture<T> to be awaited by an executor or thread pool until signaled as completed by the TB's callback.
     *
     * See BlockingRequest.java for the sync implementation.
     *
     */
    // @formatter:on

    private final CompletableFuture<TResponse> future;

    AsyncRequest(final Client client, final byte operation, final Batch batch) {
        super(client, operation, batch);

        future = new CompletableFuture<TResponse>();
    }

    public static AsyncRequest<CreateAccountResultBatch> createAccounts(final Client client,
            final AccountBatch batch) {
        return new AsyncRequest<CreateAccountResultBatch>(client,
                Request.Operations.CREATE_ACCOUNTS, batch);
    }

    public static AsyncRequest<AccountBatch> lookupAccounts(final Client client,
            final IdBatch batch) {
        return new AsyncRequest<AccountBatch>(client, Request.Operations.LOOKUP_ACCOUNTS, batch);
    }

    public static AsyncRequest<CreateTransferResultBatch> createTransfers(final Client client,
            final TransferBatch batch) {
        return new AsyncRequest<CreateTransferResultBatch>(client,
                Request.Operations.CREATE_TRANSFERS, batch);
    }

    public static AsyncRequest<TransferBatch> lookupTransfers(final Client client,
            final IdBatch batch) {
        return new AsyncRequest<TransferBatch>(client, Request.Operations.LOOKUP_TRANSFERS, batch);
    }

    public CompletableFuture<TResponse> getFuture() {
        return future;
    }

    @Override
    protected void setResult(final TResponse result) {

        // To prevent the completion to run in the callback thread
        // we must call "completeAsync" instead of "complete".
        future.completeAsync(() -> result);
    }

    @Override
    protected void setException(final Throwable exception) {
        future.completeExceptionally(exception);
    }
}
