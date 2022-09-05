package com.tigerbeetle.tests;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.management.OperationsException;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.tigerbeetle.Client;
import com.tigerbeetle.CreateTransferResult;
import com.tigerbeetle.InitializationException;
import com.tigerbeetle.RequestException;
import com.tigerbeetle.Transfer;
import com.tigerbeetle.TransferFlags;
import com.tigerbeetle.Account;

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
            try (var client = new Client(0, new String[] { Server.TB_PORT })) {

                var errors = client.createAccounts(new Account[] { account1, account2 });
                Assert.assertTrue(errors.length == 0);

                var lookupAccounts = client.lookupAccounts(new UUID[] { account1.getId(), account2.getId() });
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
            try (var client = new Client(0, new String[] { Server.TB_PORT })) {

                var errors = client.createAccounts(new Account[] { account1, account2 });
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

                var lookupAccounts = client.lookupAccounts(new UUID[] { account1.getId(), account2.getId() });
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
            try (var client = new Client(0, new String[] { Server.TB_PORT })) {

                var errors = client.createAccounts(new Account[] { account1, account2 });
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

                var lookupAccounts = client.lookupAccounts(new UUID[] { account1.getId(), account2.getId() });
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

                lookupAccounts = client.lookupAccounts(new UUID[] { account1.getId(), account2.getId() });
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
            try (var client = new Client(0, new String[] { Server.TB_PORT })) {

                var errors = client.createAccounts(new Account[] { account1, account2 });
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

                var lookupAccounts = client.lookupAccounts(new UUID[] { account1.getId(), account2.getId() });
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

                lookupAccounts = client.lookupAccounts(new UUID[] { account1.getId(), account2.getId() });
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
            try (var client = new Client(0, new String[] { Server.TB_PORT })) {

                var errors = client.createAccounts(new Account[] { account1, account2 });
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

                var transfersErrors = client.createTransfers(new Transfer[] { transfer1, transfer2 });
                Assert.assertTrue(transfersErrors.length == 0);

                var lookupAccounts = client.lookupAccounts(new UUID[] { account1.getId(), account2.getId() });
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

    @Test
    @Ignore
    public void testConcurrentTasks() throws Throwable {

        class TransferTask extends Thread {

            public final Client client;
            public CreateTransferResult result;
            public boolean isFaulted;

            public TransferTask(Client client) {
                this.client = client;
            }

            @Override
            public void run() {
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
        
        try (var server = new Server()) {

            var random = new Random();
            final int tasks_qty = 12;
            final int max_concurrency = random.nextInt(tasks_qty / 2) + 1;

            try (var client = new Client(0, new String[] { Server.TB_PORT }, max_concurrency)) {

                var errors = client.createAccounts(new Account[] { account1, account2 });
                Assert.assertTrue(errors.length == 0);

                var list = new TransferTask[tasks_qty];
                for (int i = 0; i < tasks_qty; i++) {
                    list[i] = new TransferTask(client);
                    list[i].start();
                }

                // Wait for all threads
                for (int i = 0; i < tasks_qty; i++) {
                    list[i].join();
                    Assert.assertFalse(list[i].isFaulted);
                    Assert.assertEquals(list[i].result, CreateTransferResult.Ok);
                }

                var lookupAccounts = client.lookupAccounts(new UUID[] { account1.getId(), account2.getId() });
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

    // Skip this test for now
    @Ignore
    @Test
    public void testTimeoutClient() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] { Server.TB_PORT })) {

                var errors = client.createAccounts(new Account[] { account1, account2 });
                Assert.assertTrue(errors.length == 0);

                // Closes the server
                server.close();

                try {
                    // Client will submit the request, but it is not going to complete
                    @SuppressWarnings("unused")
                    var accounts = client.lookupAccounts(new UUID[] { account1.getId(), account2.getId() });

                    // It is not expceted to lookupAccounts to finish
                    Assert.assertTrue(false);

                } catch (InterruptedException timeout) {
                    Assert.assertTrue(true);
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

    private class Server implements AutoCloseable {

        public static final String TB_EXE = "tigerbeetle";
        public static final String TB_PORT = "3001";
        public static final String TB_FILE = "./java-tests.tigerbeetle";
        public static final String TB_PATH = "src/zig/lib/tigerbeetle/";
        public static final String TB_SERVER = TB_PATH + "/" + TB_EXE;

        private Process process;

        public Server()
                throws IOException, OperationsException, InterruptedException {

            cleanUp();

            var format = Runtime.getRuntime()
                    .exec(new String[] { TB_SERVER, "format", "--cluster=0", "--replica=0", TB_FILE });
            if (format.waitFor() != 0) {
                var reader = new BufferedReader(new InputStreamReader(format.getErrorStream()));
                var error = reader.lines().collect(Collectors.joining(". "));
                throw new OperationsException("Format failed. " + error);
            }

            this.process = Runtime.getRuntime()
                    .exec(new String[] { TB_SERVER, "start", "--addresses=" + TB_PORT, TB_FILE });
            if (process.waitFor(100, TimeUnit.MICROSECONDS))
                throw new OperationsException("Start server failed");
        }

        @Override
        public void close()
                throws Exception {
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