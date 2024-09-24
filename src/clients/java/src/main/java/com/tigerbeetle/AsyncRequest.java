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

    AsyncRequest(final NativeClient nativeClient, final Operations operation, final Batch batch) {
        super(nativeClient, operation, batch);

        future = new CompletableFuture<TResponse>();
    }

    public static AsyncRequest<CreateAccountResultBatch> createAccounts(
            final NativeClient nativeClient, final AccountBatch batch) {
        return new AsyncRequest<CreateAccountResultBatch>(nativeClient,
                Request.Operations.CREATE_ACCOUNTS, batch);
    }

    public static AsyncRequest<AccountBatch> lookupAccounts(final NativeClient nativeClient,
            final IdBatch batch) {
        return new AsyncRequest<AccountBatch>(nativeClient, Request.Operations.LOOKUP_ACCOUNTS,
                batch);
    }

    public static AsyncRequest<CreateTransferResultBatch> createTransfers(
            final NativeClient nativeClient, final TransferBatch batch) {
        return new AsyncRequest<CreateTransferResultBatch>(nativeClient,
                Request.Operations.CREATE_TRANSFERS, batch);
    }

    public static AsyncRequest<TransferBatch> lookupTransfers(final NativeClient nativeClient,
            final IdBatch batch) {
        return new AsyncRequest<TransferBatch>(nativeClient, Request.Operations.LOOKUP_TRANSFERS,
                batch);
    }

    public static AsyncRequest<TransferBatch> getAccountTransfers(final NativeClient nativeClient,
            final AccountFilter filter) {
        return new AsyncRequest<TransferBatch>(nativeClient,
                Request.Operations.GET_ACCOUNT_TRANSFERS, filter.batch);
    }

    public static AsyncRequest<AccountBalanceBatch> getAccountBalances(
            final NativeClient nativeClient, final AccountFilter filter) {
        return new AsyncRequest<AccountBalanceBatch>(nativeClient,
                Request.Operations.GET_ACCOUNT_BALANCES, filter.batch);
    }

    public static AsyncRequest<AccountBatch> queryAccounts(final NativeClient nativeClient,
            final QueryFilter filter) {
        return new AsyncRequest<AccountBatch>(nativeClient, Request.Operations.QUERY_ACCOUNTS,
                filter.batch);
    }

    public static AsyncRequest<TransferBatch> queryTransfers(final NativeClient nativeClient,
            final QueryFilter filter) {
        return new AsyncRequest<TransferBatch>(nativeClient, Request.Operations.QUERY_TRANSFERS,
                filter.batch);
    }

    public static AsyncRequest<AccountBatch> echo(final NativeClient nativeClient,
            final AccountBatch batch) {
        return new AsyncRequest<AccountBatch>(nativeClient, Request.Operations.ECHO_ACCOUNTS,
                batch);
    }

    public static AsyncRequest<TransferBatch> echo(final NativeClient nativeClient,
            final TransferBatch batch) {
        return new AsyncRequest<TransferBatch>(nativeClient, Request.Operations.ECHO_TRANSFERS,
                batch);
    }

    public CompletableFuture<TResponse> getFuture() {
        return future;
    }

    @Override
    protected void setResult(final TResponse result) {
        final var completed = future.complete(result);
        if (!completed) {
            throw new IllegalStateException("Request has already been completed");
        }
    }

    @Override
    protected void setException(final Throwable exception) {
        final var completed = future.completeExceptionally(exception);
        if (!completed) {
            throw new IllegalStateException("Request has already been completed");
        }
    }
}
