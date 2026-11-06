package com.tigerbeetle;

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests using a TigerBeetle instance.
 */
public class IntegrationTest {
    private static final byte[] clusterId = new byte[16];

    private static Server server;
    private static Client client;

    private static AccountBatch generateAccounts(final byte[]... ids) {
        final var accounts = new AccountBatch(ids.length);

        for (var id : ids) {
            accounts.add();
            accounts.setId(id);
            accounts.setUserData128(100, 0);
            accounts.setUserData64(101);
            accounts.setUserData32(102);
            accounts.setLedger(720);
            accounts.setCode(1);
            accounts.setFlags(AccountFlags.NONE);
        }

        accounts.beforeFirst();
        return accounts;
    }

    @BeforeClass
    public static void initialize() throws Exception {
        server = new Server("tests");
        client = new Client(clusterId, server.getAddress());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        client.close();
        server.close();
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullReplicaAddresses() throws Throwable {
        try (final var client = new Client(clusterId, null)) {
            fail();
        }
    }

    @Test
    public void testConstructorCluster() throws Throwable {
        final var clusterId = UInt128.id();
        final var replicaAddresses = "3001";
        try (final var client = new Client(clusterId, replicaAddresses)) {
            assertArrayEquals(clusterId, client.getClusterID());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorInvalidCluster() throws Throwable {
        final var clusterIdInvalid = new byte[] {0, 0, 0};
        final var replicaAddresses = "3001";
        try (final var client = new Client(clusterIdInvalid, replicaAddresses)) {
            fail();
        }
    }

    @Test
    public void testCreateAccountTooMuchData() throws Throwable {
        final int TOO_MUCH_DATA = 10_000;
        final var accounts = new AccountBatch(TOO_MUCH_DATA);
        for (int i = 0; i < TOO_MUCH_DATA; i++) {
            accounts.add();
            accounts.setId(UInt128.id());
            accounts.setCode(1);
            accounts.setLedger(1);
        }
        assertThrows(TooMuchDataException.class, () -> client.createAccounts(accounts));
    }

    @Test
    public void testCreateAccountTooMuchDataAsync() throws Throwable {
        final int TOO_MUCH_DATA = 10_000;
        final var accounts = new AccountBatch(TOO_MUCH_DATA);
        for (int i = 0; i < TOO_MUCH_DATA; i++) {
            accounts.add();
            accounts.setId(UInt128.id());
            accounts.setCode(1);
            accounts.setLedger(1);
        }

        try {
            CompletableFuture<CreateAccountResultBatch> future =
                    client.createAccountsAsync(accounts);
            assertNotNull(future);

            future.get();
            fail();
        } catch (ExecutionException executionException) {
            assertTrue(executionException.getCause() instanceof TooMuchDataException);
        }
    }

    @Test
    public void testCreateTransferTooMuchData() throws Throwable {
        final int TOO_MUCH_DATA = 10_000;
        final var transfers = new TransferBatch(TOO_MUCH_DATA);

        for (int i = 0; i < TOO_MUCH_DATA; i++) {
            transfers.add();
            transfers.setId(UInt128.id());
            transfers.setDebitAccountId(UInt128.id());
            transfers.setDebitAccountId(UInt128.id());
            transfers.setCode(1);
            transfers.setLedger(1);
        }
        assertThrows(TooMuchDataException.class, () -> client.createTransfers(transfers));

    }

    @Test
    public void testCreateTransferTooMuchDataAsync() throws Throwable {
        final int TOO_MUCH_DATA = 10_000;
        final var transfers = new TransferBatch(TOO_MUCH_DATA);

        for (int i = 0; i < TOO_MUCH_DATA; i++) {
            transfers.add();
            transfers.setId(UInt128.id());
            transfers.setDebitAccountId(UInt128.id());
            transfers.setDebitAccountId(UInt128.id());
            transfers.setCode(1);
            transfers.setLedger(1);
        }

        try {
            CompletableFuture<CreateTransferResultBatch> future =
                    client.createTransfersAsync(transfers);
            assertNotNull(future);

            future.get();
            fail();

        } catch (ExecutionException executionException) {
            assertTrue(executionException.getCause() instanceof TooMuchDataException);
        }
    }

    @Test
    public void testClientEvicted() throws Throwable {
        final int CLIENTS_MAX = 64;

        final var barrier = new CountDownLatch(CLIENTS_MAX);
        final var executor = Executors.newFixedThreadPool(CLIENTS_MAX);

        // Use a separate server to avoid evicting the test's shared client.
        try (final var server = new Server("testClientEvicted")) {

            try (final var client_evict = new Client(clusterId, server.getAddress())) {
                var accounts_first = client_evict.lookupAccounts(new IdBatch(UInt128.id()));
                assertTrue(accounts_first.getLength() == 0);

                for (int i = 0; i < CLIENTS_MAX; i++) {
                    executor.submit(() -> {
                        try (final var client = new Client(clusterId, server.getAddress())) {
                            var accounts = client.lookupAccounts(new IdBatch(UInt128.id()));
                            assertTrue(accounts.getLength() == 0);
                        } catch (InterruptedException e) {
                            return;
                        } finally {
                            barrier.countDown();
                        }
                    });
                }

                barrier.await();
                executor.shutdown();
                assertThrows(ClientEvictedException.class,
                        () -> client_evict.lookupAccounts(new IdBatch(UInt128.id())));

                assertThrows(ClientEvictedException.class,
                        () -> client_evict.lookupAccounts(new IdBatch(UInt128.id())));

                client_evict.close();

                assertThrows(ClientClosedException.class,
                        () -> client_evict.lookupAccounts(new IdBatch(UInt128.id())));
            }
        }
    }

    /**
     * This test asserts that a linked chain is consistent across concurrent requests.
     */
    @Test
    public void testConcurrentLinkedChainsTasks() throws Throwable {
        final int TASKS_COUNT = 256;
        final int BATCH_SIZE = 64;
        final var barrier = new CountDownLatch(TASKS_COUNT);

        try (final var client = new Client(clusterId, server.getAddress())) {
            final var account1Id = UInt128.id();
            final var account2Id = UInt128.id();
            final var accounts = generateAccounts(account1Id, account2Id);

            final var accountResults = client.createAccounts(accounts);
            assertHeader(accounts, accountResults);
            assertEquals(accounts.getLength(), accountResults.getLength());
            assertTrue(createSuccess(accountResults));

            final var tasks = new TransferTask[TASKS_COUNT];
            for (int i = 0; i < TASKS_COUNT; i++) {
                // Starting multiple threads submitting transfers.
                // The Linked flag will cause the
                // batch to fail due to LinkedEventChainOpen.
                final var flag = i % 10 == 0 ? TransferFlags.LINKED : TransferFlags.NONE;
                tasks[i] = new TransferTask(client, BATCH_SIZE, account1Id, account2Id, flag,
                        barrier, new CountDownLatch(0));
                tasks[i].start();
            }

            // Wait for all threads:
            for (int i = 0; i < TASKS_COUNT; i++) {
                tasks[i].join();
                var batch = tasks[i].result;
                for (int j = 0; j < BATCH_SIZE; j++) {
                    assertTrue(batch.next());
                    assertTrue(batch.getTimestamp() > 0);

                    if (i % 10 == 0) {
                        final var status =
                                j == BATCH_SIZE - 1 ? CreateTransferStatus.LinkedEventChainOpen
                                        : CreateTransferStatus.LinkedEventFailed;
                        assertTrue(batch.getStatus() == status);
                    } else {

                        assertTrue(batch.getStatus() == CreateTransferStatus.Created);
                    }
                }
                assertFalse(batch.next());
            }
        }
    }

    /**
     * This test asserts that client.close() will wait for all ongoing request to complete And new
     * threads trying to submit a request after the client was closed will fail with
     * IllegalStateException.
     */
    @Test
    public void testCloseWithConcurrentTasks() throws Throwable {
        // The goal here is to queue many concurrent requests,
        // so we have a good chance of testing "client.close()" not only before/after
        // calling "submit()", but also during the native call.
        //
        // Unfortunately this is a hacky test, but a reasonable one:
        // Since our JNI module does not expose the acquire_batch function,
        // we cannot insert a lock/wait in between "acquire_batch" and "submit"
        // in order to cause and assert each variant.
        final int TASKS_COUNT = 256;
        final int BATCH_SIZE = 64;
        final var enterBarrier = new CountDownLatch(TASKS_COUNT);
        final var exitBarrier = new CountDownLatch(1);

        try (final var client = new Client(clusterId, server.getAddress())) {
            final var account1Id = UInt128.id();
            final var account2Id = UInt128.id();
            final var accounts = generateAccounts(account1Id, account2Id);

            final var accountResults = client.createAccounts(accounts);
            assertHeader(accounts, accountResults);
            assertEquals(accounts.getLength(), accountResults.getLength());
            assertTrue(createSuccess(accountResults));

            final var tasks = new TransferTask[TASKS_COUNT];
            for (int i = 0; i < TASKS_COUNT; i++) {

                // Starting multiple threads submitting transfers.
                tasks[i] = new TransferTask(client, BATCH_SIZE, account1Id, account2Id,
                        TransferFlags.NONE, enterBarrier, exitBarrier);
                tasks[i].start();
            }

            // Waits until one thread finish.
            exitBarrier.await();

            // And then close the client while threads are still working
            // Some of them have already submitted the request, while others will fail
            // due to "shutdown".
            client.close();

            int failedCount = 0;
            int succeededCount = 0;

            for (final var task : tasks) {

                // The client.close must wait until all submitted requests have completed
                // Asserting that either the task succeeded or failed while waiting.
                task.join();

                final var succeeded = task.result != null && createSuccess(task.result);

                // Can fail due to client closed.
                final var failed =
                        task.exception != null && task.exception instanceof ClientClosedException;

                assertTrue(failed || succeeded);

                if (failed) {
                    failedCount += 1;
                } else if (succeeded) {
                    succeededCount += 1;
                    assertTrue(createSuccess(task.result));
                }
            }

            assertTrue(succeededCount + failedCount == TASKS_COUNT);
        }
    }

    /**
     * Smoke test that concurrent close does not crash the JVM.
     */
    @Test
    public void testCloseConcurrent() throws Throwable {
        final int threadCount = 16;
        final var clients = IntStream.range(0, threadCount)
                .mapToObj((index) -> new Client(clusterId, server.getAddress()))
                .collect(Collectors.toList());
        final var threads = IntStream.range(0, threadCount).mapToObj((index) -> {
            final var thread = new Thread(() -> {
                final var client = clients.get(index);
                for (int i = 0; i < 1000; i++) {
                    try {
                        client.createAccounts(generateAccounts(UInt128.id()));
                    } catch (IllegalStateException | InterruptedException e) {
                        break;
                    }
                }
            });
            thread.start();
            return thread;
        }).collect(Collectors.toList());
        for (var client : clients) {
            client.close();
        }
        for (var thread : threads) {
            thread.join();
        }
    }

    /**
     * This test asserts that async calls will not block.
     */
    @Test
    public void testAsyncTasks() throws Throwable {
        final int TASKS_COUNT = 1_000_000;

        try (final var client = new Client(clusterId, server.getAddress())) {

            final var account1Id = UInt128.id();
            final var account2Id = UInt128.id();
            final var accounts = generateAccounts(account1Id, account2Id);

            final var accountResults = client.createAccounts(accounts);
            assertHeader(accounts, accountResults);
            assertEquals(accounts.getLength(), accountResults.getLength());
            assertTrue(createSuccess(accountResults));

            final var tasks = new CompletableFuture[TASKS_COUNT];
            for (int i = 0; i < TASKS_COUNT; i += 2) {

                final var transfers = new TransferBatch(1);
                transfers.add();

                transfers.setId(UInt128.id());
                transfers.setCreditAccountId(account1Id);
                transfers.setDebitAccountId(account2Id);
                transfers.setLedger(720);
                transfers.setCode(1);
                transfers.setAmount(100);

                // Starting two async requests of different operations.
                tasks[i] = client.createTransfersAsync(transfers);
                tasks[i + 1] = client.lookupAccountsAsync(new IdBatch(account1Id));
            }

            // Wait for all tasks.
            CompletableFuture.allOf(tasks).join();

            for (int i = 0; i < TASKS_COUNT; i++) {
                if (i % 2 == 0) {
                    @SuppressWarnings("unchecked")
                    final var future = (CompletableFuture<CreateTransferResultBatch>) tasks[i];
                    final var result = future.get();
                    assertEquals(1, result.getLength());
                    assertTrue(createSuccess(result));
                } else {
                    @SuppressWarnings("unchecked")
                    final var future = (CompletableFuture<AccountBatch>) tasks[i];
                    final var result = future.get();
                    assertEquals(1, result.getLength());
                }
            }

            // Asserting if all transfers were submitted correctly.
            final var lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
            assertEquals(2, lookupAccounts.getLength());

            accounts.beforeFirst();

            assertTrue(accounts.next());
            assertTrue(lookupAccounts.next());
            assertAccounts(accounts, lookupAccounts);

            assertEquals(BigInteger.valueOf(100 * (TASKS_COUNT / 2)),
                    lookupAccounts.getCreditsPosted());
            assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

            assertTrue(accounts.next());
            assertTrue(lookupAccounts.next());
            assertAccounts(accounts, lookupAccounts);

            assertEquals(BigInteger.valueOf(100 * (TASKS_COUNT / 2)),
                    lookupAccounts.getDebitsPosted());
            assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        }
    }

    /**
     * This test asserts that the client can handle thread interruption.
     */
    @Test
    public void testConcurrentInterruptedTasks() throws Throwable {
        final int TASKS_COUNT = 256;
        final int BATCH_SIZE = 64;
        final var barrier = new CountDownLatch(TASKS_COUNT);

        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var accounts = generateAccounts(account1Id, account2Id);

        final var accountResults = client.createAccounts(accounts);
        assertHeader(accounts, accountResults);
        assertEquals(accounts.getLength(), accountResults.getLength());
        assertTrue(createSuccess(accountResults));

        final var tasks = new TransferTask[TASKS_COUNT];
        for (int i = 0; i < TASKS_COUNT; i++) {
            // Starting multiple threads.
            tasks[i] = new TransferTask(client, BATCH_SIZE, account1Id, account2Id,
                    TransferFlags.NONE, barrier, new CountDownLatch(0));
            tasks[i].start();
        }

        // Waits until all threads start.
        barrier.await();

        int interruptedCount = 0;
        int succeededCount = 0;

        // Interrupt all threads.
        for (final var task : tasks) {
            task.interrupt();
            task.join();

            assertTrue(task.getState() == Thread.State.TERMINATED);
            if (task.result != null) {
                succeededCount += 1;
                assertTrue(createSuccess(task.result));
            } else {
                interruptedCount += 1;
                assertTrue(task.exception != null);
                assertTrue(task.exception instanceof InterruptedException);
            }
        }

        assertTrue(interruptedCount + succeededCount == TASKS_COUNT);
    }

    @Test
    public void testConcurrentQueries() throws Throwable {
        final var filterCriteria = UInt128.id();
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var transferId = UInt128.id();
        {
            // Creating the accounts and transfers.
            final var accounts = new AccountBatch(2);
            accounts.add();
            accounts.setId(account1Id);
            accounts.setUserData128(filterCriteria);
            accounts.setCode(1);
            accounts.setLedger(720);
            accounts.setFlags(AccountFlags.HISTORY);

            accounts.add();
            accounts.setId(account2Id);
            accounts.setCode(1);
            accounts.setLedger(720);

            final var accountResults = client.createAccounts(accounts);
            assertHeader(accounts, accountResults);
            assertEquals(accounts.getLength(), accountResults.getLength());
            assertTrue(createSuccess(accountResults));

            final var transfers = new TransferBatch(1);
            transfers.add();
            transfers.setId(transferId);
            transfers.setUserData128(filterCriteria);
            transfers.setCreditAccountId(account1Id);
            transfers.setDebitAccountId(account2Id);
            transfers.setLedger(720);
            transfers.setCode((short) 1);
            transfers.setAmount(100);

            final var transferResults = client.createTransfers(transfers);
            assertHeader(transfers, transferResults);
            assertEquals(transfers.getLength(), transferResults.getLength());
            assertTrue(createSuccess(transferResults));
        }

        final class Tasks {
            CompletableFuture<AccountBatch> queryAccounts;
            CompletableFuture<TransferBatch> queryTransfers;
            CompletableFuture<TransferBatch> getAccountTransfers;
            CompletableFuture<AccountBalanceBatch> getAccountBalances;

            void validate() throws ExecutionException, InterruptedException {
                CompletableFuture.allOf(queryAccounts, queryTransfers, getAccountBalances,
                        getAccountTransfers).join();

                validateAccount(queryAccounts.get());
                validateTransfer(queryTransfers.get());
                validateTransfer(getAccountTransfers.get());
                validateAccountBalances(getAccountBalances.get());
            }

            private void validateAccount(AccountBatch results) {
                assertEquals(1, results.getLength());
                assertTrue(results.next());
                assertArrayEquals(results.getId(), account1Id);
            }

            private void validateTransfer(TransferBatch results) {
                assertEquals(1, results.getLength());
                assertTrue(results.next());
                assertArrayEquals(results.getId(), transferId);
            }

            private void validateAccountBalances(AccountBalanceBatch results) {
                assertEquals(1, results.getLength());
                assertTrue(results.next());
            }
        }

        // Limit=1 allows multiple queries to be batched together in the same request.
        // Limit=8189 prevents batching because it would exceed the message size.
        final var TASKS_COUNT = 100;
        final var limits = new int[] {1, 8189};
        for (var limit : limits) {
            var tasks = new Tasks[TASKS_COUNT];
            for (int i = 0; i < TASKS_COUNT; i++) {
                tasks[i] = new Tasks();

                final var queryFilter = new QueryFilter();
                queryFilter.setUserData128(filterCriteria);
                queryFilter.setLimit(limit);
                tasks[i].queryAccounts = client.queryAccountsAsync(queryFilter);
                tasks[i].queryTransfers = client.queryTransfersAsync(queryFilter);

                final var accountFilter = new AccountFilter();
                accountFilter.setAccountId(account1Id);
                accountFilter.setUserData128(filterCriteria);
                accountFilter.setCredits(true);
                accountFilter.setLimit(limit);
                tasks[i].getAccountTransfers = client.getAccountTransfersAsync(accountFilter);
                tasks[i].getAccountBalances = client.getAccountBalancesAsync(accountFilter);
            }

            for (var task : tasks) {
                task.validate();
            }
        }
    }

    /**
     * Asserts that empty replies are not shared.
     * https://github.com/tigerbeetle/tigerbeetle/pull/2495
     */
    @Test
    public void testEmptyReply() throws Throwable {
        final int TASKS_COUNT = 100;

        try (final var client2 = new Client(clusterId, server.getAddress())) {
            for (int i = 0; i < TASKS_COUNT; i++) {
                var request1 = client.lookupAccountsAsync(new IdBatch(UInt128.id()));
                var request2 = client2.lookupAccountsAsync(new IdBatch(UInt128.id()));

                var reply1 = request1.get();
                var reply2 = request2.get();
                assertTrue(reply1.getLength() == 0);
                assertTrue(reply1.isReadOnly());
                assertTrue(reply2.getLength() == 0);
                assertTrue(reply2.isReadOnly());

                assertNotSame(reply1, reply2);
            }
        }
    }

    private static void assertAccounts(AccountBatch account1, AccountBatch account2) {
        assertArrayEquals(account1.getId(), account2.getId());
        assertArrayEquals(account1.getUserData128(), account2.getUserData128());
        assertEquals(account1.getUserData64(), account2.getUserData64());
        assertEquals(account1.getUserData32(), account2.getUserData32());
        assertEquals(account1.getLedger(), account2.getLedger());
        assertEquals(account1.getCode(), account2.getCode());
        assertEquals(account1.getFlags(), account2.getFlags());
    }

    private static void assertHeader(Batch request, Batch response) {
        assertNull(request.getHeader());
        assertNotNull(response.getHeader());
        assertTrue(response.getHeader().getTimestamp() != 0L);
    }

    boolean createSuccess(Batch batch) {
        try {
            var hasError = false;
            while (batch.next()) {
                if (batch instanceof CreateAccountResultBatch) {
                    final var accountResults = (CreateAccountResultBatch) batch;
                    assertTrue(accountResults.getTimestamp() > 0);
                    hasError |= accountResults.getStatus() != CreateAccountStatus.Created;
                } else if (batch instanceof CreateTransferResultBatch) {
                    final var transferResults = (CreateTransferResultBatch) batch;
                    assertTrue(transferResults.getTimestamp() > 0);
                    hasError |= transferResults.getStatus() != CreateTransferStatus.Created;
                } else
                    fail();
            }
            return !hasError;
        } finally {
            batch.beforeFirst();
        }
    }

    private static class TransferTask extends Thread {
        public final Client client;
        public CreateTransferResultBatch result;
        public Throwable exception;
        private int batch_size;
        private byte[] account1Id;
        private byte[] account2Id;
        private int flags;
        private CountDownLatch enterBarrier;
        private CountDownLatch exitBarrier;

        public TransferTask(Client client, int batch_size, byte[] account1Id, byte[] account2Id,
                int flags, CountDownLatch enterBarrier, CountDownLatch exitBarrier) {
            this.client = client;
            this.result = null;
            this.exception = null;
            this.batch_size = batch_size;
            this.account1Id = account1Id;
            this.account2Id = account2Id;
            this.flags = flags;
            this.enterBarrier = enterBarrier;
            this.exitBarrier = exitBarrier;
        }

        @Override
        public synchronized void run() {

            final var transfers = new TransferBatch(batch_size);
            for (int i = 0; i < batch_size; i++) {
                transfers.add();
                transfers.setId(UInt128.id());
                transfers.setCreditAccountId(account1Id);
                transfers.setDebitAccountId(account2Id);
                transfers.setLedger(720);
                transfers.setCode(1);
                transfers.setAmount(100);
                transfers.setFlags(flags);
            }

            try {
                enterBarrier.countDown();
                enterBarrier.await();
                result = client.createTransfers(transfers);
            } catch (Throwable any) {
                exception = any;
            } finally {
                exitBarrier.countDown();
            }
        }
    }
}
