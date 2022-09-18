package com.tigerbeetle;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.OperationsException;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Integration tests using a tigerbeetle instance
 */
public class IntegrationTest {

    private static Account account1;
    private static Account account2;

    static {
        account1 = new Account();
        account1.setId(UUID.randomUUID());
        account1.setUserData(UUID.randomUUID());
        account1.setLedger(720);
        account1.setCode(1);

        account2 = new Account();
        account2.setId(UUID.randomUUID());
        account2.setUserData(UUID.randomUUID());
        account2.setLedger(720);
        account2.setCode(2);
    }

    @Test
    public void testCreateAccounts() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var errors = client.createAccounts(new Account[] {account1, account2});
                Assert.assertTrue(errors.length == 0);

                var lookupAccounts =
                        client.lookupAccounts(new UUID[] {account1.getId(), account2.getId()});
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateTransfers() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var errors = client.createAccounts(new Account[] {account1, account2});
                Assert.assertTrue(errors.length == 0);

                var transfer = new Transfer();
                transfer.setId(UUID.randomUUID());
                transfer.setCreditAccountId(account1.getId());
                transfer.setDebitAccountId(account2.getId());
                transfer.setLedger(720);
                transfer.setCode((short) 1);
                transfer.setAmount(100);

                var result = client.createTransfer(transfer);
                Assert.assertTrue(result == CreateTransferResult.Ok);

                var lookupAccounts =
                        client.lookupAccounts(new UUID[] {account1.getId(), account2.getId()});
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(lookupAccounts[0].getCreditsPosted(), transfer.getAmount());
                Assert.assertEquals(lookupAccounts[0].getDebitsPosted(), (long) 0);

                Assert.assertEquals(lookupAccounts[1].getDebitsPosted(), transfer.getAmount());
                Assert.assertEquals(lookupAccounts[1].getCreditsPosted(), (long) 0);

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreatePendingTransfers() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var errors = client.createAccounts(new Account[] {account1, account2});
                Assert.assertTrue(errors.length == 0);

                var transfer = new Transfer();
                transfer.setId(UUID.randomUUID());
                transfer.setCreditAccountId(account1.getId());
                transfer.setDebitAccountId(account2.getId());
                transfer.setLedger(720);
                transfer.setCode((short) 1);
                transfer.setAmount(100);
                transfer.setFlags(TransferFlags.PENDING);
                transfer.setTimeout(Integer.MAX_VALUE);

                var result = client.createTransfer(transfer);
                Assert.assertTrue(result == CreateTransferResult.Ok);

                var lookupAccounts =
                        client.lookupAccounts(new UUID[] {account1.getId(), account2.getId()});
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(lookupAccounts[0].getCreditsPending(), transfer.getAmount());
                Assert.assertEquals(lookupAccounts[0].getDebitsPending(), (long) 0);
                Assert.assertEquals(lookupAccounts[0].getCreditsPosted(), (long) 0);
                Assert.assertEquals(lookupAccounts[0].getDebitsPosted(), (long) 0);

                Assert.assertEquals(lookupAccounts[1].getDebitsPending(), transfer.getAmount());
                Assert.assertEquals(lookupAccounts[1].getCreditsPending(), (long) 0);
                Assert.assertEquals(lookupAccounts[1].getDebitsPosted(), (long) 0);
                Assert.assertEquals(lookupAccounts[1].getCreditsPosted(), (long) 0);

                var postTransfer = new Transfer();
                postTransfer.setId(UUID.randomUUID());
                postTransfer.setCreditAccountId(account1.getId());
                postTransfer.setDebitAccountId(account2.getId());
                postTransfer.setLedger(720);
                postTransfer.setCode((short) 1);
                postTransfer.setAmount(100);
                postTransfer.setFlags(TransferFlags.POST_PENDING_TRANSFER);
                postTransfer.setPendingId(transfer.getId());

                var postResult = client.createTransfer(postTransfer);
                Assert.assertTrue(postResult == CreateTransferResult.Ok);

                lookupAccounts =
                        client.lookupAccounts(new UUID[] {account1.getId(), account2.getId()});
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(lookupAccounts[0].getCreditsPosted(), transfer.getAmount());
                Assert.assertEquals(lookupAccounts[0].getDebitsPosted(), (long) 0);
                Assert.assertEquals(lookupAccounts[0].getCreditsPending(), (long) 0);
                Assert.assertEquals(lookupAccounts[0].getDebitsPending(), (long) 0);

                Assert.assertEquals(lookupAccounts[1].getDebitsPosted(), transfer.getAmount());
                Assert.assertEquals(lookupAccounts[1].getCreditsPosted(), (long) 0);
                Assert.assertEquals(lookupAccounts[1].getDebitsPending(), (long) 0);
                Assert.assertEquals(lookupAccounts[1].getCreditsPending(), (long) 0);

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreatePendingTransfersAndVoid() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var errors = client.createAccounts(new Account[] {account1, account2});
                Assert.assertTrue(errors.length == 0);

                var transfer = new Transfer();
                transfer.setId(UUID.randomUUID());
                transfer.setCreditAccountId(account1.getId());
                transfer.setDebitAccountId(account2.getId());
                transfer.setLedger(720);
                transfer.setCode((short) 1);
                transfer.setAmount(100);
                transfer.setFlags(TransferFlags.PENDING);
                transfer.setTimeout(Integer.MAX_VALUE);

                var result = client.createTransfer(transfer);
                Assert.assertTrue(result == CreateTransferResult.Ok);

                var lookupAccounts =
                        client.lookupAccounts(new UUID[] {account1.getId(), account2.getId()});
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(lookupAccounts[0].getCreditsPending(), transfer.getAmount());
                Assert.assertEquals(lookupAccounts[0].getDebitsPending(), (long) 0);
                Assert.assertEquals(lookupAccounts[0].getCreditsPosted(), (long) 0);
                Assert.assertEquals(lookupAccounts[0].getDebitsPosted(), (long) 0);

                Assert.assertEquals(lookupAccounts[1].getDebitsPending(), transfer.getAmount());
                Assert.assertEquals(lookupAccounts[1].getCreditsPending(), (long) 0);
                Assert.assertEquals(lookupAccounts[1].getDebitsPosted(), (long) 0);
                Assert.assertEquals(lookupAccounts[1].getCreditsPosted(), (long) 0);

                var postTransfer = new Transfer();
                postTransfer.setId(UUID.randomUUID());
                postTransfer.setCreditAccountId(account1.getId());
                postTransfer.setDebitAccountId(account2.getId());
                postTransfer.setLedger(720);
                postTransfer.setCode((short) 1);
                postTransfer.setAmount(100);
                postTransfer.setFlags(TransferFlags.VOID_PENDING_TRANSFER);
                postTransfer.setPendingId(transfer.getId());

                var postResult = client.createTransfer(postTransfer);
                Assert.assertTrue(postResult == CreateTransferResult.Ok);

                lookupAccounts =
                        client.lookupAccounts(new UUID[] {account1.getId(), account2.getId()});
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(lookupAccounts[0].getCreditsPosted(), (long) 0);
                Assert.assertEquals(lookupAccounts[0].getDebitsPosted(), (long) 0);
                Assert.assertEquals(lookupAccounts[0].getCreditsPending(), (long) 0);
                Assert.assertEquals(lookupAccounts[0].getDebitsPending(), (long) 0);

                Assert.assertEquals(lookupAccounts[1].getDebitsPosted(), (long) 0);
                Assert.assertEquals(lookupAccounts[1].getCreditsPosted(), (long) 0);
                Assert.assertEquals(lookupAccounts[1].getDebitsPending(), (long) 0);
                Assert.assertEquals(lookupAccounts[1].getCreditsPending(), (long) 0);

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateLinkedTransfers() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var errors = client.createAccounts(new Account[] {account1, account2});
                Assert.assertTrue(errors.length == 0);

                var transfer1 = new Transfer();
                transfer1.setId(UUID.randomUUID());
                transfer1.setCreditAccountId(account1.getId());
                transfer1.setDebitAccountId(account2.getId());
                transfer1.setLedger(720);
                transfer1.setCode((short) 1);
                transfer1.setAmount(100);
                transfer1.setFlags(TransferFlags.LINKED);

                var transfer2 = new Transfer();
                transfer2.setId(UUID.randomUUID());
                transfer2.setCreditAccountId(account2.getId());
                transfer2.setDebitAccountId(account1.getId());
                transfer2.setLedger(720);
                transfer2.setCode((short) 1);
                transfer2.setAmount(49);
                transfer2.setFlags(TransferFlags.NONE);

                var transfersErrors = client.createTransfers(new Transfer[] {transfer1, transfer2});
                Assert.assertTrue(transfersErrors.length == 0);

                var lookupAccounts =
                        client.lookupAccounts(new UUID[] {account1.getId(), account2.getId()});
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(lookupAccounts[0].getCreditsPosted(), transfer1.getAmount());
                Assert.assertEquals(lookupAccounts[0].getDebitsPosted(), transfer2.getAmount());
                Assert.assertEquals(lookupAccounts[0].getCreditsPending(), (long) 0);
                Assert.assertEquals(lookupAccounts[0].getDebitsPending(), (long) 0);

                Assert.assertEquals(lookupAccounts[1].getCreditsPosted(), transfer2.getAmount());
                Assert.assertEquals(lookupAccounts[1].getDebitsPosted(), transfer1.getAmount());
                Assert.assertEquals(lookupAccounts[1].getCreditsPending(), (long) 0);
                Assert.assertEquals(lookupAccounts[1].getDebitsPending(), (long) 0);

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    /**
     * This test asserts that parallel threads will respect client's maxConcurrency,
     *
     * @throws Throwable
     */
    @Test
    public void testConcurrentTasks() throws Throwable {

        try (var server = new Server()) {

            // Defining a ratio between concurrent threads and client's maxConcurrency
            // The goal here is to force to have more threads than the client can process
            // simultaneously
            var random = new Random();
            final int tasks_qty = 12;
            final int max_concurrency = random.nextInt(tasks_qty - 1) + 1;

            try (var client = new Client(0, new String[] {Server.TB_PORT}, max_concurrency)) {

                var errors = client.createAccounts(new Account[] {account1, account2});
                Assert.assertTrue(errors.length == 0);

                var tasks = new TransferTask[tasks_qty];
                for (int i = 0; i < tasks_qty; i++) {
                    // Starting multiple threads submiting transfers,
                    tasks[i] = new TransferTask(client);
                    tasks[i].start();
                }

                // Wait for all threads
                for (int i = 0; i < tasks_qty; i++) {
                    tasks[i].join();
                    Assert.assertFalse(tasks[i].isFaulted);
                    Assert.assertEquals(tasks[i].result, CreateTransferResult.Ok);
                }

                // Asserting if all transfers were submited correctly
                var lookupAccounts =
                        client.lookupAccounts(new UUID[] {account1.getId(), account2.getId()});
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(lookupAccounts[0].getCreditsPosted(), (long) (100 * tasks_qty));
                Assert.assertEquals(lookupAccounts[0].getDebitsPosted(), (long) 0);

                Assert.assertEquals(lookupAccounts[1].getDebitsPosted(), (long) (100 * tasks_qty));
                Assert.assertEquals(lookupAccounts[1].getCreditsPosted(), (long) 0);

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    /**
     * This test asserts that client.close() will wait for all ongoing request to complete And new
     * threads trying to submit a request after the client was closed will fail with
     * IllegalStateException
     *
     * @throws Throwable
     */
    @Test
    public void testCloseWithConcurrentTasks() throws Throwable {

        try (var server = new Server()) {

            var random = new Random();

            // Defining a ratio between concurrent threads and client's maxConcurrency
            // The goal here is to force to have way more threads than the client can
            // process simultaneously
            final int tasks_qty = 12;
            final int max_concurrency = random.nextInt(tasks_qty / 4) + 1;

            try (var client = new Client(0, new String[] {Server.TB_PORT}, max_concurrency)) {

                var errors = client.createAccounts(new Account[] {account1, account2});
                Assert.assertTrue(errors.length == 0);

                var tasks = new TransferTask[tasks_qty];
                for (int i = 0; i < tasks_qty; i++) {

                    // Starting multiple threads submiting transfers,
                    tasks[i] = new TransferTask(client);
                    tasks[i].start();
                }

                // Wait just for one thread to complete
                tasks[0].join();

                // And them close the client while several threads are still working
                // Some of them have already submited the request, others are waiting due to the
                // maxConcurrency limit
                client.close();

                for (int i = 0; i < tasks_qty; i++) {

                    // The client.close must wait until all submited requests have completed
                    // Asserting that either the task succeeded or failed while waiting
                    tasks[i].join();
                    Assert.assertTrue(
                            tasks[i].isFaulted || tasks[i].result == CreateTransferResult.Ok);
                }

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    private static void assertAccounts(Account account1, Account account2) {
        assertEquals(account1.getId(), account2.getId());
        assertEquals(account1.getUserData(), account2.getUserData());
        assertEquals(account1.getLedger(), account2.getLedger());
        assertEquals(account1.getCode(), account2.getCode());
        assertEquals(account1.getFlags(), account2.getFlags());
    }

    private class TransferTask extends Thread {

        public final Client client;
        public CreateTransferResult result;
        public boolean isFaulted;

        public TransferTask(Client client) {
            this.client = client;
            this.result = CreateTransferResult.Ok;
            this.isFaulted = false;
        }

        @Override
        public synchronized void run() {
            var transfer = new Transfer();
            transfer.setId(UUID.randomUUID());
            transfer.setCreditAccountId(account1.getId());
            transfer.setDebitAccountId(account2.getId());
            transfer.setLedger(720);
            transfer.setCode((short) 1);
            transfer.setAmount(100);

            try {
                result = client.createTransfer(transfer);
            } catch (Throwable e) {
                isFaulted = true;
            }
        }
    }

    private class Server implements AutoCloseable {

        public static final String TB_EXE = "tigerbeetle";
        public static final String TB_PORT = "3001";
        public static final String TB_FILE = "./java-tests.tigerbeetle";
        public static final String TB_PATH = "../zig/lib/tigerbeetle/";
        public static final String TB_SERVER = TB_PATH + "/" + TB_EXE;

        private Process process;

        public Server() throws IOException, OperationsException, InterruptedException {

            cleanUp();

            var format = Runtime.getRuntime().exec(
                    new String[] {TB_SERVER, "format", "--cluster=0", "--replica=0", TB_FILE});
            if (format.waitFor() != 0) {
                var reader = new BufferedReader(new InputStreamReader(format.getErrorStream()));
                var error = reader.lines().collect(Collectors.joining(". "));
                throw new OperationsException("Format failed. " + error);
            }

            this.process = Runtime.getRuntime()
                    .exec(new String[] {TB_SERVER, "start", "--addresses=" + TB_PORT, TB_FILE});
            if (process.waitFor(100, TimeUnit.MICROSECONDS))
                throw new OperationsException("Start server failed");
        }

        @Override
        public void close() throws Exception {
            cleanUp();
        }

        private void cleanUp() {
            try {
                if (process != null && process.isAlive()) {
                    process.destroy();
                }

                var file = new File("./" + TB_FILE);
                file.delete();
            } catch (Throwable any) {
            }
        }
    }

}
