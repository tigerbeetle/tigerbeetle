package com.tigerbeetle;

import java.util.concurrent.CompletableFuture;

final class AsyncRequest<T> extends Request<T> {

    private final CompletableFuture<T[]> future;

    private AsyncRequest(Client client, byte operation, Batch batch) {
        super(client, operation, batch);

        future = new CompletableFuture<>();
    }

    public static AsyncRequest<CreateAccountsResult> createAccounts(Client client,
            AccountsBatch batch) {
        return new AsyncRequest<CreateAccountsResult>(client, Request.Operations.CREATE_ACCOUNTS,
                batch);
    }

    public static AsyncRequest<Account> lookupAccounts(Client client, UUIDsBatch batch) {
        return new AsyncRequest<Account>(client, Request.Operations.LOOKUP_ACCOUNTS, batch);
    }

    public static AsyncRequest<CreateTransfersResult> createTransfers(Client client,
            TransfersBatch batch) {
        return new AsyncRequest<CreateTransfersResult>(client, Request.Operations.CREATE_TRANSFERS,
                batch);
    }

    public static AsyncRequest<Transfer> lookupTransfers(Client client, UUIDsBatch batch) {
        return new AsyncRequest<Transfer>(client, Request.Operations.LOOKUP_TRANSFERS, batch);
    }

    public CompletableFuture<T[]> getFuture() {
        return future;
    }

    @Override
    protected void setResult(T[] result) {
        future.complete(result);
    }

    @Override
    protected void setException(Throwable exception) {
        future.completeExceptionally(exception);
    }
}
