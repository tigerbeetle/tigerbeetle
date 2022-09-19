package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.OperationsException;

import org.junit.Assert;
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

    @Test(expected = NullPointerException.class)
    public void testConstructorNullReplicaAddresses() throws Throwable {

        try (var client = new Client(0, null)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyReplicaAddresses() throws Throwable {

        var replicaAddresses = new String[0];
        try (var client = new Client(0, replicaAddresses)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyStringReplicaAddresses() throws Throwable {

        var replicaAddresses = new String[] {"", "", ""};
        try (var client = new Client(0, replicaAddresses)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorInvalidReplicaAddresses() throws Throwable {

        var replicaAddresses = new String[] {"127.0.0.1:99999"};
        try (var client = new Client(0, replicaAddresses)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNegativeCluster() throws Throwable {

        var replicaAddresses = new String[] {"3001"};
        try (var client = new Client(-1, replicaAddresses)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNegativeMaxConcurrency() throws Throwable {

        var replicaAddresses = new String[] {"3001"};
        var maxConcurrency = -1;
        try (var client = new Client(0, replicaAddresses, maxConcurrency)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateAccountsArray() throws Throwable {

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
    public void testCreateAccountsBatch() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var accountsBatch = new AccountsBatch(2);
                accountsBatch.add(account1);
                accountsBatch.add(account2);
                var errors = client.createAccounts(accountsBatch);
                Assert.assertTrue(errors.length == 0);

                var uuidsBatch = new UUIDsBatch(2);
                uuidsBatch.add(account1.getId());
                uuidsBatch.add(account2.getId());
                var lookupAccounts = client.lookupAccounts(uuidsBatch);
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
    public void testCreateSingleAccount() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var error = client.createAccount(account1);
                Assert.assertTrue(error == CreateAccountResult.Ok);

                var lookupAccount = client.lookupAccount(account1.getId());
                assertNotNull(lookupAccount);
                assertAccounts(account1, lookupAccount);

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateInvalidAccount() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var account = new Account();
                var error = client.createAccount(account);
                Assert.assertTrue(error == CreateAccountResult.IdMustNotBeZero);

                var lookupAccount = client.lookupAccount(account.getId());
                assertNull(lookupAccount);

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateAccountsAsyncArray() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                Future<CreateAccountsResult[]> createAccountsFuture =
                        client.createAccountsAsync(new Account[] {account1, account2});
                Assert.assertFalse(createAccountsFuture.isDone());

                var errors = createAccountsFuture.get();
                Assert.assertTrue(createAccountsFuture.isDone());
                Assert.assertTrue(errors.length == 0);

                Future<Account[]> lookupAccountsFuture =
                        client.lookupAccountsAsync(new UUID[] {account1.getId(), account2.getId()});
                Assert.assertFalse(lookupAccountsFuture.isDone());

                var lookupAccounts = lookupAccountsFuture.get();
                Assert.assertTrue(lookupAccountsFuture.isDone());
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
    public void testCreateAccountsAsyncBatch() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var accountsBatch = new AccountsBatch(2);
                accountsBatch.add(account1);
                accountsBatch.add(account2);

                Future<CreateAccountsResult[]> createAccountsFuture =
                        client.createAccountsAsync(accountsBatch);
                Assert.assertFalse(createAccountsFuture.isDone());

                var errors = createAccountsFuture.get();
                Assert.assertTrue(createAccountsFuture.isDone());
                Assert.assertTrue(errors.length == 0);

                var uuidsBatch = new UUIDsBatch(2);
                uuidsBatch.add(account1.getId());
                uuidsBatch.add(account2.getId());

                Future<Account[]> lookupAccountsFuture = client.lookupAccountsAsync(uuidsBatch);
                Assert.assertFalse(lookupAccountsFuture.isDone());

                var lookupAccounts = lookupAccountsFuture.get();
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
    public void testCreateTransfersArray() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var createAccountsErrors =
                        client.createAccounts(new Account[] {account1, account2});
                Assert.assertTrue(createAccountsErrors.length == 0);

                var transfer = new Transfer();
                transfer.setId(UUID.randomUUID());
                transfer.setCreditAccountId(account1.getId());
                transfer.setDebitAccountId(account2.getId());
                transfer.setLedger(720);
                transfer.setCode((short) 1);
                transfer.setAmount(100);

                var createTransfersErrors = client.createTransfers(new Transfer[] {transfer});
                Assert.assertTrue(createTransfersErrors.length == 0);

                var lookupAccounts =
                        client.lookupAccounts(new UUID[] {account1.getId(), account2.getId()});
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(transfer.getAmount(), lookupAccounts[0].getCreditsPosted());
                Assert.assertEquals(0L, lookupAccounts[0].getDebitsPosted());

                Assert.assertEquals(transfer.getAmount(), lookupAccounts[1].getDebitsPosted());
                Assert.assertEquals(0L, lookupAccounts[1].getCreditsPosted());

                var lookupTransfers = client.lookupTransfers(new UUID[] {transfer.getId()});
                Assert.assertTrue(lookupTransfers.length == 1);

                assertTransfers(transfer, lookupTransfers[0]);
                Assert.assertNotEquals(0L, lookupTransfers[0].getTimestamp());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateTransfersBatch() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var accountsBatch = new AccountsBatch(2);
                accountsBatch.add(account1);
                accountsBatch.add(account2);
                var createAccountErrors = client.createAccounts(accountsBatch);
                Assert.assertTrue(createAccountErrors.length == 0);

                var transfer = new Transfer();
                transfer.setId(UUID.randomUUID());
                transfer.setCreditAccountId(account1.getId());
                transfer.setDebitAccountId(account2.getId());
                transfer.setLedger(720);
                transfer.setCode((short) 1);
                transfer.setAmount(100);

                var transfersBatch = new TransfersBatch(1);
                transfersBatch.add(transfer);
                var createTransferErrors = client.createTransfers(transfersBatch);
                Assert.assertTrue(createTransferErrors.length == 0);

                var accountsUUIDsBatch = new UUIDsBatch(2);
                accountsUUIDsBatch.add(account1.getId());
                accountsUUIDsBatch.add(account2.getId());
                var lookupAccounts = client.lookupAccounts(accountsUUIDsBatch);
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(transfer.getAmount(), lookupAccounts[0].getCreditsPosted());
                Assert.assertEquals(0L, lookupAccounts[0].getDebitsPosted());

                Assert.assertEquals(transfer.getAmount(), lookupAccounts[1].getDebitsPosted());
                Assert.assertEquals(0L, lookupAccounts[1].getCreditsPosted());

                var transfersUUIDsBatch = new UUIDsBatch(1);
                transfersUUIDsBatch.add(transfer.getId());
                var lookupTransfers = client.lookupTransfers(transfersUUIDsBatch);
                Assert.assertTrue(lookupTransfers.length == 1);

                assertTransfers(transfer, lookupTransfers[0]);
                Assert.assertNotEquals(0L, lookupTransfers[0].getTimestamp());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateTransfersAsyncArray() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                Future<CreateAccountsResult[]> createAccountsErrorsFuture =
                        client.createAccountsAsync(new Account[] {account1, account2});
                Assert.assertFalse(createAccountsErrorsFuture.isDone());

                var createAccountsErrors = createAccountsErrorsFuture.get();
                Assert.assertTrue(createAccountsErrors.length == 0);

                var transfer = new Transfer();
                transfer.setId(UUID.randomUUID());
                transfer.setCreditAccountId(account1.getId());
                transfer.setDebitAccountId(account2.getId());
                transfer.setLedger(720);
                transfer.setCode((short) 1);
                transfer.setAmount(100);

                Future<CreateTransfersResult[]> createTransfersErrorsFuture =
                        client.createTransfersAsync(new Transfer[] {transfer});
                Assert.assertFalse(createTransfersErrorsFuture.isDone());

                var createTransfersErrors = createTransfersErrorsFuture.get();
                Assert.assertTrue(createTransfersErrors.length == 0);

                Future<Account[]> lookupAccountsFuture =
                        client.lookupAccountsAsync(new UUID[] {account1.getId(), account2.getId()});
                Assert.assertFalse(lookupAccountsFuture.isDone());

                var lookupAccounts = lookupAccountsFuture.get();
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(transfer.getAmount(), lookupAccounts[0].getCreditsPosted());
                Assert.assertEquals(0L, lookupAccounts[0].getDebitsPosted());

                Assert.assertEquals(transfer.getAmount(), lookupAccounts[1].getDebitsPosted());
                Assert.assertEquals(0L, lookupAccounts[1].getCreditsPosted());

                Future<Transfer[]> lookupTransfersFuture =
                        client.lookupTransfersAsync(new UUID[] {transfer.getId()});
                Assert.assertFalse(lookupTransfersFuture.isDone());

                var lookupTransfers = lookupTransfersFuture.get();
                Assert.assertTrue(lookupTransfers.length == 1);

                assertTransfers(transfer, lookupTransfers[0]);
                Assert.assertNotEquals(0L, lookupTransfers[0].getTimestamp());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateTransfersAsyncBatch() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var accountsBatch = new AccountsBatch(2);
                accountsBatch.add(account1);
                accountsBatch.add(account2);
                Future<CreateAccountsResult[]> createAccountsErrorsFuture =
                        client.createAccountsAsync(accountsBatch);
                Assert.assertFalse(createAccountsErrorsFuture.isDone());

                var createAccountsErrors = createAccountsErrorsFuture.get();
                Assert.assertTrue(createAccountsErrors.length == 0);

                var transfer = new Transfer();
                transfer.setId(UUID.randomUUID());
                transfer.setCreditAccountId(account1.getId());
                transfer.setDebitAccountId(account2.getId());
                transfer.setLedger(720);
                transfer.setCode((short) 1);
                transfer.setAmount(100);

                var transfersBatch = new TransfersBatch(1);
                transfersBatch.add(transfer);

                Future<CreateTransfersResult[]> createTransferErrorsFuture =
                        client.createTransfersAsync(transfersBatch);
                Assert.assertFalse(createTransferErrorsFuture.isDone());

                var createTransferErrors = createTransferErrorsFuture.get();
                Assert.assertTrue(createTransferErrors.length == 0);

                var accountsUUIDsBatch = new UUIDsBatch(2);
                accountsUUIDsBatch.add(account1.getId());
                accountsUUIDsBatch.add(account2.getId());

                Future<Account[]> lookupAccountsFuture =
                        client.lookupAccountsAsync(accountsUUIDsBatch);
                Assert.assertFalse(lookupAccountsFuture.isDone());

                var lookupAccounts = lookupAccountsFuture.get();
                assertAccounts(account1, lookupAccounts[0]);
                assertAccounts(account2, lookupAccounts[1]);

                Assert.assertEquals(transfer.getAmount(), lookupAccounts[0].getCreditsPosted());
                Assert.assertEquals(0L, lookupAccounts[0].getDebitsPosted());

                Assert.assertEquals(transfer.getAmount(), lookupAccounts[1].getDebitsPosted());
                Assert.assertEquals(0L, lookupAccounts[1].getCreditsPosted());

                var transfersUUIDsBatch = new UUIDsBatch(1);
                transfersUUIDsBatch.add(transfer.getId());
                Future<Transfer[]> lookupTransfersFuture =
                        client.lookupTransfersAsync(transfersUUIDsBatch);
                Assert.assertFalse(lookupTransfersFuture.isDone());

                var lookupTransfers = lookupTransfersFuture.get();
                Assert.assertTrue(lookupTransfers.length == 1);

                assertTransfers(transfer, lookupTransfers[0]);
                Assert.assertNotEquals(0L, lookupTransfers[0].getTimestamp());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateSingleTransfer() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var account1Result = client.createAccount(account1);
                Assert.assertTrue(account1Result == CreateAccountResult.Ok);

                var account2Result = client.createAccount(account2);
                Assert.assertTrue(account2Result == CreateAccountResult.Ok);

                var transfer = new Transfer();
                transfer.setId(UUID.randomUUID());
                transfer.setCreditAccountId(account1.getId());
                transfer.setDebitAccountId(account2.getId());
                transfer.setLedger(720);
                transfer.setCode((short) 1);
                transfer.setAmount(100);

                var transferResult = client.createTransfer(transfer);
                Assert.assertTrue(transferResult == CreateTransferResult.Ok);

                var lookupAccount1 = client.lookupAccount(account1.getId());
                assertAccounts(account1, lookupAccount1);

                var lookupAccount2 = client.lookupAccount(account2.getId());
                assertAccounts(account2, lookupAccount2);

                Assert.assertEquals(lookupAccount1.getCreditsPosted(), transfer.getAmount());
                Assert.assertEquals(lookupAccount1.getDebitsPosted(), (long) 0);

                Assert.assertEquals(lookupAccount2.getDebitsPosted(), transfer.getAmount());
                Assert.assertEquals(lookupAccount2.getCreditsPosted(), (long) 0);

                var lookupTransfer = client.lookupTransfer(transfer.getId());
                Assert.assertNotNull(lookupTransfer);

                assertTransfers(transfer, lookupTransfer);
                Assert.assertNotEquals(0L, lookupTransfer.getTimestamp());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateInvalidTransfer() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                var transfer = new Transfer();
                var transferResult = client.createTransfer(transfer);
                Assert.assertTrue(transferResult == CreateTransferResult.IdMustNotBeZero);

                var lookupTransfer = client.lookupTransfer(transfer.getId());
                Assert.assertNull(lookupTransfer);

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

                var lookupTransfer = client.lookupTransfer(transfer.getId());
                Assert.assertNotNull(lookupTransfer);

                assertTransfers(transfer, lookupTransfer);
                Assert.assertNotEquals(0L, lookupTransfer.getTimestamp());

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

                var lookupPostTransfer = client.lookupTransfer(postTransfer.getId());
                Assert.assertNotNull(lookupPostTransfer);

                assertTransfers(postTransfer, lookupPostTransfer);
                Assert.assertNotEquals(0L, lookupPostTransfer.getTimestamp());

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

                var lookupTransfer = client.lookupTransfer(transfer.getId());
                Assert.assertNotNull(lookupTransfer);

                assertTransfers(transfer, lookupTransfer);
                Assert.assertNotEquals(0L, lookupTransfer.getTimestamp());

                var voidTransfer = new Transfer();
                voidTransfer.setId(UUID.randomUUID());
                voidTransfer.setCreditAccountId(account1.getId());
                voidTransfer.setDebitAccountId(account2.getId());
                voidTransfer.setLedger(720);
                voidTransfer.setCode((short) 1);
                voidTransfer.setAmount(100);
                voidTransfer.setFlags(TransferFlags.VOID_PENDING_TRANSFER);
                voidTransfer.setPendingId(transfer.getId());

                var postResult = client.createTransfer(voidTransfer);
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

                var lookupVoidTransfer = client.lookupTransfer(voidTransfer.getId());
                Assert.assertNotNull(lookupVoidTransfer);

                assertTransfers(voidTransfer, lookupVoidTransfer);
                Assert.assertNotEquals(0L, lookupVoidTransfer.getTimestamp());

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

                var lookupTransfers =
                        client.lookupTransfers(new UUID[] {transfer1.getId(), transfer2.getId()});
                Assert.assertEquals(2, lookupTransfers.length);

                assertTransfers(transfer1, lookupTransfers[0]);
                assertTransfers(transfer2, lookupTransfers[1]);
                Assert.assertNotEquals(0L, lookupTransfers[0].getTimestamp());
                Assert.assertNotEquals(0L, lookupTransfers[1].getTimestamp());

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

    private static void assertTransfers(Transfer transfer1, Transfer transfer2) {
        assertEquals(transfer1.getId(), transfer2.getId());
        assertEquals(transfer1.getCreditAccountId(), transfer2.getCreditAccountId());
        assertEquals(transfer1.getDebitAccountId(), transfer2.getDebitAccountId());
        assertEquals(transfer1.getUserData(), transfer2.getUserData());
        assertEquals(transfer1.getLedger(), transfer2.getLedger());
        assertEquals(transfer1.getCode(), transfer2.getCode());
        assertEquals(transfer1.getFlags(), transfer2.getFlags());
        assertEquals(transfer1.getAmount(), transfer2.getAmount());
        assertEquals(transfer1.getTimeout(), transfer2.getTimeout());
        assertEquals(transfer1.getPendingId(), transfer2.getPendingId());
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
            if (process.waitFor(100, TimeUnit.MILLISECONDS))
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
