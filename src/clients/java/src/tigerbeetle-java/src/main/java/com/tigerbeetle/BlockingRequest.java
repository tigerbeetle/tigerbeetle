package com.tigerbeetle;

final class BlockingRequest<T> extends Request<T> {

    private T[] result;
    private Throwable exception;

    private BlockingRequest(Client client, byte operation, Batch batch) {
        super(client, operation, batch);

        result = null;
        exception = null;
    }

    public static BlockingRequest<CreateAccountsResult> createAccounts(Client client,
            AccountsBatch batch) {
        return new BlockingRequest<CreateAccountsResult>(client, Request.Operations.CREATE_ACCOUNTS,
                batch);
    }

    public static BlockingRequest<Account> lookupAccounts(Client client, UInt128Batch batch) {
        return new BlockingRequest<Account>(client, Request.Operations.LOOKUP_ACCOUNTS, batch);
    }

    public static BlockingRequest<CreateTransfersResult> createTransfers(Client client,
            TransfersBatch batch) {
        return new BlockingRequest<CreateTransfersResult>(client,
                Request.Operations.CREATE_TRANSFERS, batch);
    }

    public static BlockingRequest<Transfer> lookupTransfers(Client client, UInt128Batch batch) {
        return new BlockingRequest<Transfer>(client, Request.Operations.LOOKUP_TRANSFERS, batch);
    }

    public boolean isDone() {
        return result != null || exception != null;
    }

    public T[] waitForResult() throws RequestException {

        waitForCompletionUninterruptibly();
        return getResult();
    }

    @Override
    protected void setResult(T[] result) {

        synchronized (this) {

            if (isDone()) {

                this.result = null;
                this.exception = new AssertionError(this.exception,
                        "This request has already been completed");

            } else {

                this.result = result;
                this.exception = null;
            }

            notifyAll();
        }

    }

    @Override
    protected void setException(Throwable exception) {

        synchronized (this) {
            this.result = null;
            this.exception = exception;
            notifyAll();
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

    T[] getResult() throws RequestException {

        if (result == null && exception == null)
            throw new AssertionError("Unexpected request result: result=null");

        // Handling checked and unchecked exceptions accordingly
        if (exception != null) {

            if (exception instanceof RequestException)
                throw (RequestException) exception;

            if (exception instanceof RuntimeException)
                throw (RuntimeException) exception;

            if (exception instanceof Error)
                throw (Error) exception;

            // If we can't determine the type of the exception,
            // throw a generic RuntimeException pointing as a cause
            throw new RuntimeException(exception);

        } else {

            return this.result;
        }
    }

}
