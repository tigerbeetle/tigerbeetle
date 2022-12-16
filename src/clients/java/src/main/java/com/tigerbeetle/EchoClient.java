package com.tigerbeetle;

import java.util.concurrent.CompletableFuture;

public final class EchoClient implements AutoCloseable {

    private final NativeClient nativeClient;

    public EchoClient(final int clusterID, final String replicaAddresses,
            final int maxConcurrency) {
        this.nativeClient = NativeClient.initEcho(clusterID, replicaAddresses, maxConcurrency);
    }

    public AccountBatch echo(final AccountBatch batch) throws RequestException {
        final var request = BlockingRequest.echo(this.nativeClient, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    public TransferBatch echo(final TransferBatch batch) throws RequestException {
        final var request = BlockingRequest.echo(this.nativeClient, batch);
        request.beginRequest();
        return request.waitForResult();
    }

    public CompletableFuture<AccountBatch> echoAsync(final AccountBatch batch) {
        final var request = AsyncRequest.echo(this.nativeClient, batch);
        request.beginRequest();
        return request.getFuture();
    }

    public CompletableFuture<TransferBatch> echoAsync(final TransferBatch batch) {
        final var request = AsyncRequest.echo(this.nativeClient, batch);
        request.beginRequest();
        return request.getFuture();
    }

    public void close() throws Exception {
        nativeClient.close();
    }
}
