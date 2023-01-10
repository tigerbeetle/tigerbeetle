package com.tigerbeetle;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests using a TigerBeetle instance.
 */
public class IntegrationTest {

    private static final AccountBatch accounts;
    private static final IdBatch accountIds;

    private static final byte[] account1Id;
    private static final byte[] account2Id;
    private static final byte[] transfer1Id;
    private static final byte[] transfer2Id;

    static {

        account1Id = new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        account2Id = new byte[] {2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        transfer1Id = new byte[] {10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        transfer2Id = new byte[] {20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        accounts = new AccountBatch(2);

        accounts.add();
        accounts.setId(account1Id);
        accounts.setUserData(100, 0);
        accounts.setLedger(720);
        accounts.setCode(1);

        accounts.add();
        accounts.setId(account2Id);
        accounts.setUserData(100, 0);
        accounts.setLedger(720);
        accounts.setCode(2);

        accountIds = new IdBatch(2);
        accountIds.add();
        accountIds.setId(account1Id);

        accountIds.add();
        accountIds.setId(account2Id);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullReplicaAddresses() throws Throwable {

        try (var client = new Client(0, null)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullElementReplicaAddresses() throws Throwable {

        var replicaAddresses = new String[] {"3001", null};
        try (var client = new Client(0, replicaAddresses)) {

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

    @Test
    public void testConstructorReplicaAddressesLimitExceeded() throws Throwable {
        var replicaAddresses = new String[100];
        for (int i = 0; i < replicaAddresses.length; i++)
            replicaAddresses[i] = "3000";

        try (var client = new Client(0, replicaAddresses)) {
            assert false;
        } catch (InitializationException initializationException) {
            assertEquals(InitializationStatus.AddressLimitExceeded.value,
                    initializationException.getStatus());
        }
    }

    @Test
    public void testConstructorEmptyStringReplicaAddresses() throws Throwable {
        var replicaAddresses = new String[] {"", "", ""};
        try (var client = new Client(0, replicaAddresses)) {
            assert false;
        } catch (InitializationException initializationException) {
            assertEquals(InitializationStatus.AddressInvalid.value,
                    initializationException.getStatus());
        }
    }

    @Test
    public void testConstructorInvalidReplicaAddresses() throws Throwable {

        var replicaAddresses = new String[] {"127.0.0.1:99999"};
        try (var client = new Client(0, replicaAddresses)) {
            assert false;
        } catch (InitializationException initializationException) {
            assertEquals(InitializationStatus.AddressInvalid.value,
                    initializationException.getStatus());
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
    public void testCreateAccounts() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                accounts.beforeFirst();

                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                var lookupAccounts = client.lookupAccounts(accountIds);
                assertEquals(2, lookupAccounts.getLength());

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

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

                var zeroedAccounts = new AccountBatch(1);
                zeroedAccounts.add();

                var errors = client.createAccounts(zeroedAccounts);
                assertTrue(errors.getLength() == 1);
                assertTrue(errors.next());
                assertEquals(CreateAccountResult.IdMustNotBeZero, errors.getResult());

                var ids = new IdBatch(1);
                ids.add(accounts.getId());

                var lookupAccounts = client.lookupAccounts(ids);
                assertEquals(0, lookupAccounts.getLength());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateAccountsAsync() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                accounts.beforeFirst();

                CompletableFuture<CreateAccountResultBatch> accountsFuture =
                        client.createAccountsAsync(accounts);

                var errors = accountsFuture.get();
                assertTrue(errors.getLength() == 0);

                CompletableFuture<AccountBatch> lookupFuture =
                        client.lookupAccountsAsync(accountIds);

                var lookupAccounts = lookupFuture.get();
                assertEquals(2, lookupAccounts.getLength());

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

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

                // Creating the accounts
                var createAccountErrors = client.createAccounts(accounts);
                assertTrue(createAccountErrors.getLength() == 0);

                // Creating a transfer
                var transfers = new TransferBatch(2);

                transfers.add();
                transfers.setId(transfer1Id);
                transfers.setCreditAccountId(account1Id);
                transfers.setDebitAccountId(account2Id);
                transfers.setLedger(720);
                transfers.setCode((short) 1);
                transfers.setFlags(TransferFlags.NONE);
                transfers.setAmount(100);

                var createTransferErrors = client.createTransfers(transfers);
                assertTrue(createTransferErrors.getLength() == 0);

                // Looking up the accounts
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the first account for the credit
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPending());
                assertEquals(100L, lookupAccounts.getCreditsPosted());
                assertEquals(0L, lookupAccounts.getDebitsPosted());

                // Asserting the second account for the debit
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(0L, lookupAccounts.getDebitsPending());
                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getCreditsPosted());
                assertEquals(100L, lookupAccounts.getDebitsPosted());


                // Looking up and asserting the transfer
                var ids = new IdBatch(1);
                ids.add(transfer1Id);
                var lookupTransfers = client.lookupTransfers(ids);
                assertEquals(1, lookupTransfers.getLength());

                transfers.beforeFirst();

                assertTrue(transfers.next());
                assertTrue(lookupTransfers.next());
                assertTransfers(transfers, lookupTransfers);
                assertNotEquals(0L, lookupTransfers.getTimestamp());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateTransfersAsync() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                // Creating the accounts
                CompletableFuture<CreateAccountResultBatch> future =
                        client.createAccountsAsync(accounts);

                var createAccountErrors = future.get();
                assertTrue(createAccountErrors.getLength() == 0);

                // Creating a transfer
                var transfers = new TransferBatch(2);

                transfers.add();
                transfers.setId(transfer1Id);
                transfers.setCreditAccountId(account1Id);
                transfers.setDebitAccountId(account2Id);
                transfers.setLedger(720);
                transfers.setCode((short) 1);
                transfers.setAmount(100);

                CompletableFuture<CreateTransferResultBatch> transfersFuture =
                        client.createTransfersAsync(transfers);
                var createTransferErrors = transfersFuture.get();
                assertTrue(createTransferErrors.getLength() == 0);

                // Looking up the accounts
                CompletableFuture<AccountBatch> lookupAccountsFuture =
                        client.lookupAccountsAsync(accountIds);
                var lookupAccounts = lookupAccountsFuture.get();
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the first account for the credit
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPending());
                assertEquals(100L, lookupAccounts.getCreditsPosted());
                assertEquals(0L, lookupAccounts.getDebitsPosted());

                // Asserting the second account for the debit
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(0L, lookupAccounts.getDebitsPending());
                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getCreditsPosted());
                assertEquals(100L, lookupAccounts.getDebitsPosted());

                // Looking up and asserting the transfer
                var ids = new IdBatch(1);
                ids.add(transfer1Id);

                CompletableFuture<TransferBatch> lookupTransfersFuture =
                        client.lookupTransfersAsync(ids);
                var lookupTransfers = lookupTransfersFuture.get();
                assertEquals(1, lookupTransfers.getLength());

                transfers.beforeFirst();

                assertTrue(transfers.next());
                assertTrue(lookupTransfers.next());
                assertTransfers(transfers, lookupTransfers);
                assertNotEquals(0L, lookupTransfers.getTimestamp());

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

                var zeroedTransfers = new TransferBatch(1);
                zeroedTransfers.add();

                var errors = client.createTransfers(zeroedTransfers);
                assertTrue(errors.getLength() == 1);
                assertTrue(errors.next());
                assertEquals(CreateTransferResult.IdMustNotBeZero, errors.getResult());

                var ids = new IdBatch(1);
                ids.add(accounts.getId());

                var lookupAccounts = client.lookupAccounts(ids);
                assertEquals(0, lookupAccounts.getLength());

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

                // Creating the accounts
                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                // Creating a pending transfer
                var transfers = new TransferBatch(1);
                transfers.add();

                transfers.setId(transfer1Id);
                transfers.setCreditAccountId(account1Id);
                transfers.setDebitAccountId(account2Id);
                transfers.setLedger(720);
                transfers.setCode((short) 1);
                transfers.setAmount(100);
                transfers.setFlags(TransferFlags.PENDING);
                transfers.setTimeout(Integer.MAX_VALUE);

                var transferResults = client.createTransfers(transfers);
                assertTrue(transferResults.getLength() == 0);

                // Looking up the accounts
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the first account for the pending credit
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(100L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPending());
                assertEquals(0L, lookupAccounts.getCreditsPosted());
                assertEquals(0L, lookupAccounts.getDebitsPosted());

                // Asserting the second account for the pending debit
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(100L, lookupAccounts.getDebitsPending());
                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPosted());
                assertEquals(0L, lookupAccounts.getCreditsPosted());

                // Looking up and asserting the pending transfer
                var ids = new IdBatch(1);
                ids.add(transfer1Id);
                var lookupTransfers = client.lookupTransfers(ids);
                assertEquals(1, lookupTransfers.getLength());

                transfers.beforeFirst();

                assertTrue(transfers.next());
                assertTrue(lookupTransfers.next());
                assertTransfers(transfers, lookupTransfers);
                assertNotEquals(0L, lookupTransfers.getTimestamp());

                // Creating a post_pending transfer
                var confirmTransfers = new TransferBatch(1);
                confirmTransfers.add();
                confirmTransfers.setId(transfer2Id);
                confirmTransfers.setCreditAccountId(account1Id);
                confirmTransfers.setDebitAccountId(account2Id);
                confirmTransfers.setLedger(720);
                confirmTransfers.setCode((short) 1);
                confirmTransfers.setAmount(100);
                confirmTransfers.setFlags(TransferFlags.POST_PENDING_TRANSFER);
                confirmTransfers.setPendingId(transfer1Id);

                var postResults = client.createTransfers(confirmTransfers);
                assertEquals(0, postResults.getLength());

                // Looking up the accounts again for the updated balance
                lookupAccounts = client.lookupAccounts(accountIds);
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the pending credit was posted for the first account
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPending());
                assertEquals(100L, lookupAccounts.getCreditsPosted());
                assertEquals(0L, lookupAccounts.getDebitsPosted());


                // Asserting the pending debit was posted for the second account
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPending());
                assertEquals(0L, lookupAccounts.getCreditsPosted());
                assertEquals(100L, lookupAccounts.getDebitsPosted());

                // Looking up and asserting the post_pending transfer
                ids = new IdBatch(1);
                ids.add(transfer2Id);
                var lookupVoidTransfers = client.lookupTransfers(ids);
                assertEquals(1, lookupVoidTransfers.getLength());

                confirmTransfers.beforeFirst();

                assertTrue(confirmTransfers.next());
                assertTrue(lookupVoidTransfers.next());
                assertTransfers(confirmTransfers, lookupVoidTransfers);
                assertNotEquals(0L, lookupVoidTransfers.getTimestamp());

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

                // Creating the accounts
                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                // Creating a pending transfer
                var transfers = new TransferBatch(1);
                transfers.add();

                transfers.setId(transfer1Id);
                transfers.setCreditAccountId(account1Id);
                transfers.setDebitAccountId(account2Id);
                transfers.setLedger(720);
                transfers.setCode((short) 1);
                transfers.setAmount(100);
                transfers.setFlags(TransferFlags.PENDING);
                transfers.setTimeout(Integer.MAX_VALUE);

                var transferResults = client.createTransfers(transfers);
                assertTrue(transferResults.getLength() == 0);

                // Looking up the accounts
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the first account for the pending credit
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(100L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPending());
                assertEquals(0L, lookupAccounts.getCreditsPosted());
                assertEquals(0L, lookupAccounts.getDebitsPosted());

                // Asserting the second account for the pending credit
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(100L, lookupAccounts.getDebitsPending());
                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPosted());
                assertEquals(0L, lookupAccounts.getCreditsPosted());

                // Looking up and asserting the pending transfer
                var ids = new IdBatch(1);
                ids.add(transfer1Id);
                var lookupTransfers = client.lookupTransfers(ids);
                assertEquals(1, lookupTransfers.getLength());

                transfers.beforeFirst();

                assertTrue(transfers.next());
                assertTrue(lookupTransfers.next());
                assertTransfers(transfers, lookupTransfers);
                assertNotEquals(0L, lookupTransfers.getTimestamp());

                // Creating a void_pending transfer
                var voidTransfers = new TransferBatch(2);
                voidTransfers.add();
                voidTransfers.setId(transfer2Id);
                voidTransfers.setCreditAccountId(account1Id);
                voidTransfers.setDebitAccountId(account2Id);
                voidTransfers.setLedger(720);
                voidTransfers.setCode((short) 1);
                voidTransfers.setAmount(100);
                voidTransfers.setFlags(TransferFlags.VOID_PENDING_TRANSFER);
                voidTransfers.setPendingId(transfer1Id);

                var voidResults = client.createTransfers(voidTransfers);
                assertEquals(0, voidResults.getLength());

                // Looking up the accounts again for the updated balance
                lookupAccounts = client.lookupAccounts(accountIds);
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the pending credit was voided for the first account
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPending());
                assertEquals(0L, lookupAccounts.getCreditsPosted());
                assertEquals(0L, lookupAccounts.getDebitsPosted());


                // Asserting the pending debit was voided for the second account
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPending());
                assertEquals(0L, lookupAccounts.getCreditsPosted());
                assertEquals(0L, lookupAccounts.getDebitsPosted());

                // Looking up and asserting the void_pending transfer
                ids = new IdBatch(1);
                ids.add(transfer2Id);
                var lookupVoidTransfers = client.lookupTransfers(ids);
                assertEquals(1, lookupVoidTransfers.getLength());

                voidTransfers.beforeFirst();

                assertTrue(voidTransfers.next());
                assertTrue(lookupVoidTransfers.next());
                assertTransfers(voidTransfers, lookupVoidTransfers);
                assertNotEquals(0L, lookupVoidTransfers.getTimestamp());

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

                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                var transfers = new TransferBatch(2);
                transfers.add();
                transfers.setId(transfer1Id);
                transfers.setCreditAccountId(account1Id);
                transfers.setDebitAccountId(account2Id);
                transfers.setLedger(720);
                transfers.setCode((short) 1);
                transfers.setAmount(100);
                transfers.setFlags(TransferFlags.LINKED);

                transfers.add();
                transfers.setId(transfer2Id);
                transfers.setCreditAccountId(account2Id);
                transfers.setDebitAccountId(account1Id);
                transfers.setLedger(720);
                transfers.setCode((short) 1);
                transfers.setAmount(49);
                transfers.setFlags(TransferFlags.NONE);

                var transfersErrors = client.createTransfers(transfers);
                assertTrue(transfersErrors.getLength() == 0);

                var lookupAccounts = client.lookupAccounts(accountIds);
                assertEquals(2, lookupAccounts.getLength());

                accounts.beforeFirst();

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(100L, lookupAccounts.getCreditsPosted());
                assertEquals(49L, lookupAccounts.getDebitsPosted());
                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPending());

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(49L, lookupAccounts.getCreditsPosted());
                assertEquals(100L, lookupAccounts.getDebitsPosted());
                assertEquals(0L, lookupAccounts.getCreditsPending());
                assertEquals(0L, lookupAccounts.getDebitsPending());


                var lookupIds = new IdBatch(2);
                lookupIds.add(transfer1Id);
                lookupIds.add(transfer2Id);

                var lookupTransfers = client.lookupTransfers(lookupIds);
                assertEquals(2, lookupTransfers.getLength());

                transfers.beforeFirst();

                assertTrue(transfers.next());
                assertTrue(lookupTransfers.next());
                assertTransfers(transfers, lookupTransfers);
                assertNotEquals(0L, lookupTransfers.getTimestamp());

                assertTrue(transfers.next());
                assertTrue(lookupTransfers.next());

                assertTransfers(transfers, lookupTransfers);
                assertNotEquals(0L, lookupTransfers.getTimestamp());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateAccountTooMuchData() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                final int TOO_MUCH_DATA = 10000;
                var accounts = new AccountBatch(TOO_MUCH_DATA);
                for (int i = 0; i < TOO_MUCH_DATA; i++) {

                    accounts.add();
                    accounts.setId(UInt128.asBytes(UUID.randomUUID()));
                    accounts.setCode(1);
                    accounts.setLedger(1);

                }

                try {
                    client.createAccounts(accounts);
                    assert false;
                } catch (RequestException requestException) {

                    assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());

                }

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateAccountTooMuchDataAsync() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                final int TOO_MUCH_DATA = 10000;
                var accounts = new AccountBatch(TOO_MUCH_DATA);
                for (int i = 0; i < TOO_MUCH_DATA; i++) {

                    accounts.add();
                    accounts.setId(UInt128.asBytes(UUID.randomUUID()));
                    accounts.setCode(1);
                    accounts.setLedger(1);

                }

                try {
                    CompletableFuture<CreateAccountResultBatch> future =
                            client.createAccountsAsync(accounts);
                    assert future != null;

                    future.get();
                    assert false;

                } catch (ExecutionException executionException) {

                    assertTrue(executionException.getCause() instanceof RequestException);

                    var requestException = (RequestException) executionException.getCause();
                    assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());

                }

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateTransferTooMuchData() throws Throwable {

        try (var server = new Server()) {

            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                final int TOO_MUCH_DATA = 10000;
                var transfers = new TransferBatch(TOO_MUCH_DATA);

                for (int i = 0; i < TOO_MUCH_DATA; i++) {
                    transfers.add();
                    transfers.setId(UInt128.asBytes(UUID.randomUUID()));
                    transfers.setDebitAccountId(account1Id);
                    transfers.setDebitAccountId(account2Id);
                    transfers.setCode(1);
                    transfers.setLedger(1);
                }

                try {
                    client.createTransfers(transfers);
                    assert false;
                } catch (RequestException requestException) {

                    assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());

                }

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateTransferTooMuchDataAsync() throws Throwable {

        try (var server = new Server()) {

            try (var client = new Client(0, new String[] {Server.TB_PORT})) {

                final int TOO_MUCH_DATA = 10000;
                var transfers = new TransferBatch(TOO_MUCH_DATA);

                for (int i = 0; i < TOO_MUCH_DATA; i++) {
                    transfers.add();
                    transfers.setId(UInt128.asBytes(UUID.randomUUID()));
                    transfers.setDebitAccountId(account1Id);
                    transfers.setDebitAccountId(account2Id);
                    transfers.setCode(1);
                    transfers.setLedger(1);
                }

                try {

                    CompletableFuture<CreateTransferResultBatch> future =
                            client.createTransfersAsync(transfers);
                    assert future != null;

                    future.get();
                    assert false;

                } catch (ExecutionException executionException) {

                    assertTrue(executionException.getCause() instanceof RequestException);

                    var requestException = (RequestException) executionException.getCause();
                    assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());

                }

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    /**
     * This test asserts that parallel threads will respect client's maxConcurrency.
     */
    @Test
    public void testConcurrentTasks() throws Throwable {

        try (var server = new Server()) {

            // Defining a ratio between concurrent threads and client's maxConcurrency
            // The goal here is to force to have more threads than the client can process
            // simultaneously
            final int tasks_qty = 20;
            final int max_concurrency = tasks_qty / 2;

            try (var client = new Client(0, new String[] {Server.TB_PORT}, max_concurrency)) {

                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                var tasks = new TransferTask[tasks_qty];
                for (int i = 0; i < tasks_qty; i++) {
                    // Starting multiple threads submitting transfers,
                    tasks[i] = new TransferTask(client);
                    tasks[i].start();
                }

                // Wait for all threads
                for (int i = 0; i < tasks_qty; i++) {
                    tasks[i].join();
                    assertTrue(tasks[i].exception == null);
                    assertEquals(0, tasks[i].result.getLength());
                }

                // Asserting if all transfers were submitted correctly
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertEquals(2, lookupAccounts.getLength());

                accounts.beforeFirst();

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals((long) (100 * tasks_qty), lookupAccounts.getCreditsPosted());
                assertEquals(0L, lookupAccounts.getDebitsPosted());

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals((long) (100 * tasks_qty), lookupAccounts.getDebitsPosted());
                assertEquals(0L, lookupAccounts.getCreditsPosted());

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
     * IllegalStateException.
     */
    @Test
    public void testCloseWithConcurrentTasks() throws Throwable {

        try (var server = new Server()) {

            // The goal here is to force to have way more threads than the client can
            // process simultaneously
            final int tasks_qty = 20;
            final int max_concurrency = 2;

            try (var client = new Client(0, new String[] {Server.TB_PORT}, max_concurrency)) {

                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                var tasks = new TransferTask[tasks_qty];
                synchronized (client) {

                    for (int i = 0; i < tasks_qty; i++) {

                        // Starting multiple threads submitting transfers,
                        tasks[i] = new TransferTask(client);
                        tasks[i].start();
                    }

                    // Waiting for any thread to complete
                    client.wait();
                }

                // And then close the client while several other threads are still working
                // Some of them have already submitted the request, others are waiting due to the
                // maxConcurrency limit
                client.close();

                int failedCount = 0;
                int succeededCount = 0;

                for (int i = 0; i < tasks_qty; i++) {

                    // The client.close must wait until all submitted requests have completed
                    // Asserting that either the task succeeded or failed while waiting
                    tasks[i].join();

                    final var failed = tasks[i].exception != null
                            && tasks[i].exception.getMessage().equals("Client is closed");
                    final var succeeded =
                            tasks[i].result != null && tasks[i].result.getLength() == 0;

                    assertTrue(failed || succeeded);

                    if (failed) {
                        failedCount += 1;
                    } else if (succeeded) {
                        succeededCount += 1;
                    }
                }

                assertTrue(failedCount > 0);
                assertTrue(succeededCount > 0);
                assertTrue(succeededCount + failedCount == tasks_qty);


            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    /**
     * This test asserts that async tasks will respect client's maxConcurrency.
     */
    @Test
    public void testAsyncTasks() throws Throwable {

        try (var server = new Server()) {

            // Defining the maxConcurrency greater than tasks_qty
            // The goal here is to allow to all requests being submitted at once simultaneously
            final int tasks_qty = 100;

            try (var client = new Client(0, new String[] {Server.TB_PORT}, tasks_qty)) {

                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                final var tasks = new CompletableFuture[tasks_qty];
                for (int i = 0; i < tasks_qty; i++) {

                    final var transfers = new TransferBatch(1);
                    transfers.add();

                    transfers.setId(UInt128.asBytes(UUID.randomUUID()));
                    transfers.setCreditAccountId(account1Id);
                    transfers.setDebitAccountId(account2Id);
                    transfers.setLedger(720);
                    transfers.setCode((short) 1);
                    transfers.setAmount(100);

                    // Starting async batch
                    tasks[i] = client.createTransfersAsync(transfers);
                }

                // Wait for all threads
                for (int i = 0; i < tasks_qty; i++) {
                    @SuppressWarnings("unchecked")
                    final var future = (CompletableFuture<CreateTransferResultBatch>) tasks[i];
                    final var result = future.get();
                    assertEquals(0, result.getLength());
                }

                // Asserting if all transfers were submitted correctly
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertEquals(2, lookupAccounts.getLength());

                accounts.beforeFirst();

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals((long) (100 * tasks_qty), lookupAccounts.getCreditsPosted());
                assertEquals(0L, lookupAccounts.getDebitsPosted());

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals((long) (100 * tasks_qty), lookupAccounts.getDebitsPosted());
                assertEquals(0L, lookupAccounts.getCreditsPosted());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    private static void assertAccounts(AccountBatch account1, AccountBatch account2) {

        assertArrayEquals(account1.getId(), account2.getId());
        assertArrayEquals(account1.getUserData(), account2.getUserData());
        assertEquals(account1.getLedger(), account2.getLedger());
        assertEquals(account1.getCode(), account2.getCode());
        assertEquals(account1.getFlags(), account2.getFlags());
    }

    private static void assertTransfers(TransferBatch transfer1, TransferBatch transfer2) {
        assertArrayEquals(transfer1.getId(), transfer2.getId());
        assertArrayEquals(transfer1.getCreditAccountId(), transfer2.getCreditAccountId());
        assertArrayEquals(transfer1.getDebitAccountId(), transfer2.getDebitAccountId());
        assertArrayEquals(transfer1.getUserData(), transfer2.getUserData());
        assertEquals(transfer1.getLedger(), transfer2.getLedger());
        assertEquals(transfer1.getCode(), transfer2.getCode());
        assertEquals(transfer1.getFlags(), transfer2.getFlags());
        assertEquals(transfer1.getAmount(), transfer2.getAmount());
        assertEquals(transfer1.getTimeout(), transfer2.getTimeout());
        assertArrayEquals(transfer1.getPendingId(), transfer2.getPendingId());
    }

    private static class TransferTask extends Thread {

        public final Client client;
        public CreateTransferResultBatch result;
        public Throwable exception;

        public TransferTask(Client client) {
            this.client = client;
            this.result = null;
            this.exception = null;
        }

        @Override
        public synchronized void run() {

            var transfers = new TransferBatch(1);
            transfers.add();

            transfers.setId(UInt128.asBytes(UUID.randomUUID()));
            transfers.setCreditAccountId(account1Id);
            transfers.setDebitAccountId(account2Id);
            transfers.setLedger(720);
            transfers.setCode((short) 1);
            transfers.setAmount(100);

            try {
                result = client.createTransfers(transfers);
            } catch (Throwable e) {
                exception = e;
            } finally {

                // Signal the caller
                synchronized (client) {
                    client.notify();
                }

            }
        }
    }

    private static class Server implements AutoCloseable {

        public static final String TB_PORT = "3001";
        public static final String TB_FILE = "./0_0.tigerbeetle.tests";
        public static final String TB_SERVER = "../../../zig-out/bin/tigerbeetle";

        private Process process;

        public Server() throws IOException, Exception, InterruptedException {

            cleanUp();

            String exe;
            switch (JNILoader.OS.getOS()) {
                case windows:
                    exe = TB_SERVER + ".exe";
                    break;
                default:
                    exe = TB_SERVER;
                    break;
            }

            var format = Runtime.getRuntime()
                    .exec(new String[] {exe, "format", "--cluster=0", "--replica=0", TB_FILE});
            if (format.waitFor() != 0) {
                var reader = new BufferedReader(new InputStreamReader(format.getErrorStream()));
                var error = reader.lines().collect(Collectors.joining(". "));
                throw new Exception("Format failed. " + error);
            }

            this.process = Runtime.getRuntime()
                    .exec(new String[] {exe, "start", "--addresses=" + TB_PORT, TB_FILE});
            if (process.waitFor(100, TimeUnit.MILLISECONDS))
                throw new Exception("Start server failed");
        }

        @Override
        public void close() throws Exception {
            cleanUp();
        }

        private void cleanUp() throws Exception {
            try {

                if (process != null && process.isAlive()) {
                    process.destroy();
                }

                var file = new File("./" + TB_FILE);
                file.delete();
            } catch (Throwable any) {
                throw new Exception("Cleanup has failed");
            }
        }
    }
}
