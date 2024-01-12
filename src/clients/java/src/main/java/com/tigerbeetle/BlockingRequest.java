package com.tigerbeetle;

import static com.tigerbeetle.AssertionError.assertTrue;

final class BlockingRequest<TResponse extends Batch> extends Request<TResponse> {

    // @formatter:off
    /*
     * Overview:
     *
     * Implements a Request that blocks the caller thread until signaled as completed by the TB's callback.
     * See AsyncRequest.java for the async implementation.
     *
     * We could have used the same AsyncRequest implementation by just waiting the CompletableFuture<T>.
     *
     * CompletableFuture<T> implements a sophisticated lock using CAS + waiter stack:
     * https://hg.openjdk.java.net/jdk8/jdk8/jdk/file/687fd7c7986d/src/share/classes/java/util/concurrent/CompletableFuture.java#l114
     *
     * This BlockingRequest<T> implements a much simpler general-purpose "synchronized" block that relies on the
     * standard Monitor.wait().
     *
     * This approach is particularly good here for 3 reasons:
     *
     *   1. We are always dealing with just one waiter thread, no need for a waiter stack.
     *   2. It is expected for a request to be at least 2 io-ticks long, making sense to suspend the waiter thread immediately.
     *   3. To avoid putting more pressure on the GC with additional object allocations required by the CompletableFuture
     *
     */
    // @formatter:on

    private TResponse result;
    private Throwable exception;

    BlockingRequest(final NativeClient nativeClient, final Operations operation,
            final Batch batch) {
        super(nativeClient, operation, batch);

        result = null;
        exception = null;
    }

    public static BlockingRequest<CreateAccountResultBatch> createAccounts(
            final NativeClient nativeClient, final AccountBatch batch) {
        return new BlockingRequest<CreateAccountResultBatch>(nativeClient,
                Request.Operations.CREATE_ACCOUNTS, batch);
    }

    public static BlockingRequest<AccountBatch> lookupAccounts(final NativeClient nativeClient,
            final IdBatch batch) {
        return new BlockingRequest<AccountBatch>(nativeClient, Request.Operations.LOOKUP_ACCOUNTS,
                batch);
    }

    public static BlockingRequest<CreateTransferResultBatch> createTransfers(
            final NativeClient nativeClient, final TransferBatch batch) {
        return new BlockingRequest<CreateTransferResultBatch>(nativeClient,
                Request.Operations.CREATE_TRANSFERS, batch);
    }

    public static BlockingRequest<TransferBatch> lookupTransfers(final NativeClient nativeClient,
            final IdBatch batch) {
        return new BlockingRequest<TransferBatch>(nativeClient, Request.Operations.LOOKUP_TRANSFERS,
                batch);
    }

    public static BlockingRequest<TransferBatch> getAccountTransfers(
            final NativeClient nativeClient, final AccountTransfers filter) {
        return new BlockingRequest<TransferBatch>(nativeClient,
                Request.Operations.GET_ACCOUNT_TRANSFERS, filter.batch);
    }

    public static BlockingRequest<AccountBatch> echo(final NativeClient nativeClient,
            final AccountBatch batch) {
        return new BlockingRequest<AccountBatch>(nativeClient, Request.Operations.ECHO_ACCOUNTS,
                batch);
    }

    public static BlockingRequest<TransferBatch> echo(final NativeClient nativeClient,
            final TransferBatch batch) {
        return new BlockingRequest<TransferBatch>(nativeClient, Request.Operations.ECHO_TRANSFERS,
                batch);
    }

    public boolean isDone() {
        return result != null || exception != null;
    }

    public TResponse waitForResult() {

        waitForCompletionUninterruptibly();
        return getResult();
    }

    @Override
    protected void setResult(final TResponse result) {

        synchronized (this) {

            if (isDone()) {

                this.result = null;
                this.exception = new AssertionError(this.exception,
                        "This request has already been completed");

            } else {

                this.result = result;
                this.exception = null;
            }

            notify();
        }

    }

    @Override
    protected void setException(final Throwable exception) {

        synchronized (this) {
            this.result = null;
            this.exception = exception;
            notify();
        }

    }

    private void waitForCompletionUninterruptibly() {
        try {

            if (!isDone()) {
                synchronized (this) {
                    while (!isDone()) {
                        wait();
                    }
                }
            }

        } catch (InterruptedException interruptedException) {
            // Since we don't support canceling an ongoing request
            // this exception should never exposed by the API to be handled by the user
            throw new AssertionError(interruptedException,
                    "Unexpected thread interruption on waitForCompletion.");
        }
    }

    TResponse getResult() {

        assertTrue(result != null || exception != null, "Unexpected request result: result=null");

        // Handling checked and unchecked exceptions accordingly
        if (exception != null) {

            if (exception instanceof RequestException)
                throw (RequestException) exception;

            if (exception instanceof RuntimeException)
                throw (RuntimeException) exception;

            if (exception instanceof Error)
                throw (Error) exception;

            throw new AssertionError(exception, "Unexpected exception");

        } else {

            return this.result;
        }
    }

}
