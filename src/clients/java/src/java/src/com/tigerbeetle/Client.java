package com.tigerbeetle;

import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

public final class Client implements AutoCloseable {
    static {
        System.loadLibrary("tb_jniclient");
    }

    private static final int DEFAULT_MAX_CONCURRENCY = 32;

    private final int clusterID;
    private final Semaphore maxConcurrencySemaphore;
    private long clientHandle;
    private long packetsHead;
    private long packetsTail;

    public Client(int clusterID, String[] addresses, int maxConcurrency) throws IllegalArgumentException, Exception {
        if (clusterID < 0)
            throw new IllegalArgumentException("clusterID must be positive");
        if (addresses == null || addresses.length == 0)
            throw new IllegalArgumentException("Invalid addresses");

        // Cap the maximum amount of packets
        if (maxConcurrency <= 0)
            throw new IllegalArgumentException("Invalid maxConcurrency");
        if (maxConcurrency > 4096)
            maxConcurrency = 4096;

        var joiner = new StringJoiner(",");
        for (var address : addresses) {
            joiner.add(address);
        }

        this.clusterID = clusterID;
        int status = clientInit(clusterID, joiner.toString(), maxConcurrency);
        if (status != 0)
            throw new Exception("result " + status);

        this.maxConcurrencySemaphore = new Semaphore(maxConcurrency);
    }

    @Override
    public void close() throws Exception {
        if (clientHandle != 0) {
            clientDeinit();
            clientHandle = 0;
            packetsHead = 0;
            packetsTail = 0;
        }
    }

    public CreateAccountResult createAccount(Account account) throws InterruptedException, RequestException {
        var batch = new AccountsBatch(1);
        batch.Add(account);

        CreateAccountsResult[] results = createAccounts(batch);
        if (results.length == 0) {
            return CreateAccountResult.Ok;
        } else {
            return results[0].result;
        }
    }

    public CreateAccountsResult[] createAccounts(Account[] batch) throws InterruptedException, RequestException {
        return createAccounts(new AccountsBatch(batch));
    }

    public CreateAccountsResult[] createAccounts(AccountsBatch batch) throws InterruptedException, RequestException {
        var request = new CreateAccountsRequest(this, batch);
        request.beginRequest();
        request.waitForCompletion();
        return request.getResult();
    }

    public Future<CreateAccountsResult[]> createAccountsAsync(Account[] batch) throws InterruptedException {
        return createAccountsAsync(new AccountsBatch(batch));
    }

    public Future<CreateAccountsResult[]> createAccountsAsync(AccountsBatch batch) throws InterruptedException {
        var request = new CreateAccountsRequest(this, batch);
        request.beginRequest();
        return request;
    }

    public Account lookupAccount(UUID uuid) throws InterruptedException, RequestException {
        var batch = new UUIDsBatch(1);
        batch.Add(uuid);

        Account[] results = lookupAccounts(batch);
        if (results.length == 0) {
            return null;
        } else {
            return results[0];
        }
    }

    public Account[] lookupAccounts(UUID[] batch) throws InterruptedException, RequestException {
        return lookupAccounts(new UUIDsBatch(batch));
    }

    public Account[] lookupAccounts(UUIDsBatch batch) throws InterruptedException, RequestException {
        var request = new LookupAccountsRequest(this, batch);
        request.beginRequest();
        request.waitForCompletion();
        return request.getResult();
    }

    public Future<Account[]> lookupAccountsAsync(UUID[] batch) throws InterruptedException {
        return lookupAccountsAsync(new UUIDsBatch(batch));
    }

    public Future<Account[]> lookupAccountsAsync(UUIDsBatch batch) throws InterruptedException {
        var request = new LookupAccountsRequest(this, batch);
        request.beginRequest();
        return request;
    }

    public CreateTransferResult createTransfer(Transfer transfer) throws InterruptedException, RequestException {
        var batch = new TransfersBatch(1);
        batch.Add(transfer);

        CreateTransfersResult[] results = createTransfers(batch);
        if (results.length == 0) {
            return CreateTransferResult.Ok;
        } else {
            return results[0].result;
        }
    }

    public CreateTransfersResult[] createTransfers(Transfer[] batch) throws InterruptedException, RequestException {
        return createTransfers(new TransfersBatch(batch));
    }

    public CreateTransfersResult[] createTransfers(TransfersBatch batch) throws InterruptedException, RequestException {
        var request = new CreateTransfersRequest(this, batch);
        request.beginRequest();
        request.waitForCompletion();
        return request.getResult();
    }

    public Future<CreateTransfersResult[]> createTransfersAsync(Transfer[] batch) throws InterruptedException {
        return createTransfersAsync(new TransfersBatch(batch));
    }

    public Future<CreateTransfersResult[]> createTransfersAsync(TransfersBatch batch) throws InterruptedException {
        var request = new CreateTransfersRequest(this, batch);
        request.beginRequest();
        return request;
    }

    public Transfer lookupTransfer(UUID uuid) throws InterruptedException, RequestException {
        var batch = new UUIDsBatch(1);
        batch.Add(uuid);

        Transfer[] results = lookupTransfers(batch);
        if (results.length == 0) {
            return null;
        } else {
            return results[0];
        }
    }

    public Transfer[] lookupTransfers(UUID[] batch) throws InterruptedException, RequestException {
        return lookupTransfers(new UUIDsBatch(batch));
    }

    public Transfer[] lookupTransfers(UUIDsBatch batch) throws InterruptedException, RequestException {
        var request = new LookupTransfersRequest(this, batch);
        request.beginRequest();
        request.waitForCompletion();
        return request.getResult();
    }

    public Future<Transfer[]> lookupTransfersAsync(UUID[] batch) throws InterruptedException {
        return lookupTransfersAsync(new UUIDsBatch(batch));
    }

    public Future<Transfer[]> lookupTransfersAsync(UUIDsBatch batch) throws InterruptedException {
        var request = new LookupTransfersRequest(this, batch);
        request.beginRequest();
        return request;
    }

    long adquirePacket() throws InterruptedException {

        // Assure that only the max number of concurrent requests can adquire a packet
        // It forces other threads to wait until a packet became available
        maxConcurrencySemaphore.acquire();

        synchronized (this) {
            return popPacket();
        }

    }

    void returnPacket(long packet) {
        synchronized (this) {
            pushPacket(packet);
        }

        // Releasing the packet to be used by another thread
        maxConcurrencySemaphore.release();
    }

    private native int clientInit(int clusterID, String addresses, int maxConcurrency);

    private native void clientDeinit();

    private native long popPacket();

    private native void pushPacket(long packet);

    public static void main(String[] args) {
        try (var client = new Client(0, new String[] { "127.0.0.1:3001" }, 32)) {

            var accounts = new AccountsBatch(2);

            var account1 = new Account();
            account1.setId(UUID.randomUUID());
            account1.setCode(100);
            account1.setLedger(720);
            accounts.Add(account1);

            var account2 = new Account();
            account2.setId(UUID.randomUUID());
            account2.setCode(200);
            account2.setLedger(720);
            accounts.Add(account2);

            var results = client.createAccounts(accounts);
            if (results.length > 0)
                throw new Exception("Unexpected createAccount results");

            final int max_batches = 3;
            final int max_transfers_per_batch = 8191; // config.message_size_max - @sizeOf(vsr.Header)
            var batches = new TransfersBatch[3];
            for (int i = 0; i < max_batches; i++) {
                var batch = new TransfersBatch(max_transfers_per_batch);
                for (int j = 0; j < max_transfers_per_batch; j++) {
                    var transfer = new Transfer();
                    transfer.setId(new UUID(i + 1, j + 1));
                    transfer.setCreditAccountId(account1.getId());
                    transfer.setDebitAccountId(account2.getId());
                    transfer.setCode((short) 1);
                    transfer.setLedger(720);
                    transfer.setAmount(100);
                    batch.Add(transfer);
                }

                batches[i] = batch;
            }

            long totalTime = 0;
            long maxTransferLatency = 0;

            for (var batch : batches) {

                var now = System.currentTimeMillis();

                // Async usage:
                // Start the batch ...
                var request = client.createTransfersAsync(batch);

                // Register something on the application's side while tigerbeetle is processing
                // it
                // UPDATE FROM MyCustomer ...

                var errors = request.get();
                if (errors.length > 0)
                    throw new Exception("Unexpected transfer results");

                var elapsed = System.currentTimeMillis() - now;

                totalTime += elapsed;
                if (elapsed > maxTransferLatency)
                    maxTransferLatency = elapsed;
            }

            System.out.println("============================================");

            var result = (long) (max_batches * max_transfers_per_batch * 1000) / totalTime;

            System.out.printf("%d transfers per second\n", result);
            System.out.printf("create_transfers max p100 latency per %d transfers = %dms\n", max_transfers_per_batch,
                    maxTransferLatency);
            System.out.printf("total %d transfers in %dms\n", max_batches * max_transfers_per_batch, totalTime);

            System.console().readLine();

        } catch (Exception | RequestException e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }

}