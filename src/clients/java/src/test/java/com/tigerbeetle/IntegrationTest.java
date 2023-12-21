package com.tigerbeetle;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.math.BigInteger;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
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

    private static final byte[] clusterId;
    private static final byte[] account1Id;
    private static final byte[] account2Id;
    private static final byte[] transfer1Id;
    private static final byte[] transfer2Id;

    static {
        clusterId = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        account1Id = new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        account2Id = new byte[] {2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        transfer1Id = new byte[] {10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        transfer2Id = new byte[] {20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        accounts = new AccountBatch(2);

        accounts.add();
        accounts.setId(account1Id);
        accounts.setUserData128(100, 0);
        accounts.setUserData64(101);
        accounts.setUserData32(102);
        accounts.setLedger(720);
        accounts.setCode(1);

        accounts.add();
        accounts.setId(account2Id);
        accounts.setUserData128(200, 0);
        accounts.setUserData64(201);
        accounts.setUserData32(202);
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

        try (var client = new Client(clusterId, null)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullElementReplicaAddresses() throws Throwable {

        var replicaAddresses = new String[] {"3001", null};
        try (var client = new Client(clusterId, replicaAddresses)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyReplicaAddresses() throws Throwable {

        var replicaAddresses = new String[0];
        try (var client = new Client(clusterId, replicaAddresses)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testConstructorReplicaAddressesLimitExceeded() throws Throwable {
        var replicaAddresses = new String[100];
        for (int i = 0; i < replicaAddresses.length; i++)
            replicaAddresses[i] = "3000";

        try (var client = new Client(clusterId, replicaAddresses)) {
            assert false;
        } catch (InitializationException initializationException) {
            assertEquals(InitializationStatus.AddressLimitExceeded.value,
                    initializationException.getStatus());
        }
    }

    @Test
    public void testConstructorEmptyStringReplicaAddresses() throws Throwable {
        var replicaAddresses = new String[] {"", "", ""};
        try (var client = new Client(clusterId, replicaAddresses)) {
            assert false;
        } catch (InitializationException initializationException) {
            assertEquals(InitializationStatus.AddressInvalid.value,
                    initializationException.getStatus());
        }
    }

    @Test
    public void testConstructorInvalidReplicaAddresses() throws Throwable {

        var replicaAddresses = new String[] {"127.0.0.1:99999"};
        try (var client = new Client(clusterId, replicaAddresses)) {
            assert false;
        } catch (InitializationException initializationException) {
            assertEquals(InitializationStatus.AddressInvalid.value,
                    initializationException.getStatus());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorInvalidCluster() throws Throwable {

        var clusterIdInvalid = new byte[] {0, 0, 0};
        var replicaAddresses = new String[] {"3001"};
        try (var client = new Client(clusterIdInvalid, replicaAddresses)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNegativeConcurrencyMax() throws Throwable {

        var replicaAddresses = new String[] {"3001"};
        var concurrencyMax = -1;
        try (var client = new Client(clusterId, replicaAddresses, concurrencyMax)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testCreateAccounts() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

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
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

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
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

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
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

                // Creating the accounts.
                var createAccountErrors = client.createAccounts(accounts);
                assertTrue(createAccountErrors.getLength() == 0);

                // Creating a transfer.
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

                // Looking up the accounts.
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the first account for the credit.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

                // Asserting the second account for the debit.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPosted());

                // Looking up and asserting the transfer.
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
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

                // Creating the accounts.
                CompletableFuture<CreateAccountResultBatch> future =
                        client.createAccountsAsync(accounts);

                var createAccountErrors = future.get();
                assertTrue(createAccountErrors.getLength() == 0);

                // Creating a transfer.
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

                // Looking up the accounts.
                CompletableFuture<AccountBatch> lookupAccountsFuture =
                        client.lookupAccountsAsync(accountIds);
                var lookupAccounts = lookupAccountsFuture.get();
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the first account for the credit.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

                // Asserting the second account for the debit.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPosted());

                // Looking up and asserting the transfer.
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
                assertNotEquals(BigInteger.ZERO, lookupTransfers.getTimestamp());

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
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

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
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

                // Creating the accounts.
                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                // Creating a pending transfer.
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

                // Looking up the accounts.
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the first account for the pending credit.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

                // Asserting the second account for the pending debit.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());

                // Looking up and asserting the pending transfer.
                var ids = new IdBatch(1);
                ids.add(transfer1Id);
                var lookupTransfers = client.lookupTransfers(ids);
                assertEquals(1, lookupTransfers.getLength());

                transfers.beforeFirst();

                assertTrue(transfers.next());
                assertTrue(lookupTransfers.next());
                assertTransfers(transfers, lookupTransfers);
                assertNotEquals(0L, lookupTransfers.getTimestamp());

                // Creating a post_pending transfer.
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

                // Looking up the accounts again for the updated balance.
                lookupAccounts = client.lookupAccounts(accountIds);
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the pending credit was posted for the first account.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

                // Asserting the pending debit was posted for the second account.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPosted());

                // Looking up and asserting the post_pending transfer.
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
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

                // Creating the accounts.
                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                // Creating a pending transfer.
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

                // Looking up the accounts.
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the first account for the pending credit.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

                // Asserting the second account for the pending credit.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());

                // Looking up and asserting the pending transfer.
                var ids = new IdBatch(1);
                ids.add(transfer1Id);
                var lookupTransfers = client.lookupTransfers(ids);
                assertEquals(1, lookupTransfers.getLength());

                transfers.beforeFirst();

                assertTrue(transfers.next());
                assertTrue(lookupTransfers.next());
                assertTransfers(transfers, lookupTransfers);
                assertNotEquals(0L, lookupTransfers.getTimestamp());

                // Creating a void_pending transfer.
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

                // Looking up the accounts again for the updated balance.
                lookupAccounts = client.lookupAccounts(accountIds);
                assertTrue(lookupAccounts.getLength() == 2);

                accounts.beforeFirst();

                // Asserting the pending credit was voided for the first account.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

                // Asserting the pending debit was voided for the second account.
                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

                // Looking up and asserting the void_pending transfer.
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
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

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

                assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.valueOf(49), lookupAccounts.getDebitsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(49), lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());

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
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

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
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

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

            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

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

            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

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
     * This test asserts that the client can handle parallel threads up to concurrencyMax.
     */
    @Test
    public void testConcurrentTasks() throws Throwable {

        try (var server = new Server()) {
            // Defining the concurrency_max equals to tasks_qty
            // The goal here is to allow to all requests being submitted at once.
            final int max_concurrency = 100;
            final int tasks_qty = max_concurrency;
            final var barrier = new CountDownLatch(tasks_qty);

            try (var client =
                    new Client(clusterId, new String[] {server.getAddress()}, max_concurrency)) {

                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                var tasks = new TransferTask[tasks_qty];
                for (int i = 0; i < tasks_qty; i++) {
                    // Starting multiple threads submitting transfers.
                    tasks[i] = new TransferTask(client, barrier, new CountDownLatch(0));
                    tasks[i].start();
                }

                // Wait for all threads:
                for (int i = 0; i < tasks_qty; i++) {
                    tasks[i].join();
                    assertTrue(tasks[i].result.getLength() == 0);
                }

                // Asserting if all transfers were submitted correctly.
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertEquals(2, lookupAccounts.getLength());

                accounts.beforeFirst();

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(100 * tasks_qty),
                        lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(100 * tasks_qty), lookupAccounts.getDebitsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    /**
     * This test asserts that parallel threads will respect client's concurrencyMax.
     */
    @Test
    public void testConcurrencyExceeded() throws Throwable {

        try (var server = new Server()) {

            // Defining a ratio between concurrent threads and client's concurrencyMax
            // The goal here is to force to have more threads than the client can process
            // simultaneously.
            final int tasks_qty = 32;
            final int max_concurrency = 2;
            final var barrier = new CountDownLatch(tasks_qty);

            try (var client =
                    new Client(clusterId, new String[] {server.getAddress()}, max_concurrency)) {

                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                var tasks = new TransferTask[tasks_qty];
                for (int i = 0; i < tasks_qty; i++) {
                    // Starting multiple threads submitting transfers.
                    tasks[i] = new TransferTask(client, barrier, new CountDownLatch(0));
                    tasks[i].start();
                }

                // Wait for all threads:
                int succeededCount = 0;
                int failedCount = 0;
                for (int i = 0; i < tasks_qty; i++) {
                    tasks[i].join();
                    if (tasks[i].exception == null) {
                        assertTrue(tasks[i].result.getLength() == 0);
                        succeededCount += 1;
                    } else {
                        assertEquals(ConcurrencyExceededException.class,
                                tasks[i].exception.getClass());
                        failedCount += 1;
                    }
                }

                // At least max_concurrency tasks must succeed.
                assertTrue(succeededCount >= max_concurrency);
                assertTrue(succeededCount + failedCount == tasks_qty);

                // Asserting if all transfers were submitted correctly.
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertEquals(2, lookupAccounts.getLength());

                accounts.beforeFirst();

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(100 * succeededCount),
                        lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(100 * succeededCount),
                        lookupAccounts.getDebitsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());

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

            // The goal here is to queue many concurrent requests,
            // so we have a good chance of testing "client.close()" not only before/after
            // calling "submit()", but also during the native call.
            //
            // Unfortunately this is a hacky test, but a reasonable one:
            // Since our JNI module does not expose the acquire_packet function,
            // we cannot insert a lock/wait in between "acquire_packet" and "submit"
            // in order to cause and assert each variant.
            final int tasks_qty = 256;
            final int max_concurrency = tasks_qty;
            final var enterBarrier = new CountDownLatch(tasks_qty);
            final var exitBarrier = new CountDownLatch(1);

            try (var client =
                    new Client(clusterId, new String[] {server.getAddress()}, max_concurrency)) {

                var errors = client.createAccounts(accounts);
                assertTrue(errors.getLength() == 0);

                var tasks = new TransferTask[tasks_qty];
                for (int i = 0; i < tasks_qty; i++) {

                    // Starting multiple threads submitting transfers.
                    tasks[i] = new TransferTask(client, enterBarrier, exitBarrier);
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

                for (int i = 0; i < tasks_qty; i++) {

                    // The client.close must wait until all submitted requests have completed
                    // Asserting that either the task succeeded or failed while waiting.
                    tasks[i].join();

                    final var succeeded =
                            tasks[i].result != null && tasks[i].result.getLength() == 0;

                    // Can fail due to client closed.
                    final var failed = tasks[i].exception != null
                            && tasks[i].exception instanceof IllegalStateException;

                    assertTrue(failed || succeeded);

                    if (failed) {
                        failedCount += 1;
                    } else if (succeeded) {
                        succeededCount += 1;
                    }
                }

                assertTrue(succeededCount + failedCount == tasks_qty);

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    /**
     * This test asserts that submit a request after the client was closed will fail with
     * IllegalStateException.
     */
    @Test
    public void testClose() throws Throwable {

        try (var server = new Server()) {
            // As we call client.close() explicitly,
            // we don't need to use a try-with-resources block here.
            var client = new Client(clusterId, new String[] {server.getAddress()});

            // Creating accounts.
            var createAccountErrors = client.createAccounts(accounts);
            assertTrue(createAccountErrors.getLength() == 0);

            // Closing the client.
            client.close();

            // Creating a transfer with a closed client.
            var transfers = new TransferBatch(2);

            transfers.add();
            transfers.setId(transfer1Id);
            transfers.setCreditAccountId(account1Id);
            transfers.setDebitAccountId(account2Id);
            transfers.setLedger(720);
            transfers.setCode((short) 1);
            transfers.setFlags(TransferFlags.NONE);
            transfers.setAmount(100);

            client.createTransfers(transfers);
            assert false;

        } catch (Throwable any) {
            assertEquals(IllegalStateException.class, any.getClass());
        }
    }

    /**
     * This test asserts that async tasks will respect client's concurrencyMax.
     */
    @Test
    public void testAsyncTasks() throws Throwable {

        try (var server = new Server()) {
            // Defining the concurrency_max equals to tasks_qty
            // The goal here is to allow to all requests being submitted at once.
            final int tasks_qty = 100;
            final int concurrency_max = tasks_qty;

            try (var client =
                    new Client(clusterId, new String[] {server.getAddress()}, concurrency_max)) {

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

                    // Starting async batch.
                    tasks[i] = client.createTransfersAsync(transfers);
                }

                // Wait for all threads.
                for (int i = 0; i < tasks_qty; i++) {
                    @SuppressWarnings("unchecked")
                    final var future = (CompletableFuture<CreateTransferResultBatch>) tasks[i];
                    final var result = future.get();
                    assertEquals(0, result.getLength());
                }

                // Asserting if all transfers were submitted correctly.
                var lookupAccounts = client.lookupAccounts(accountIds);
                assertEquals(2, lookupAccounts.getLength());

                accounts.beforeFirst();

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(100 * tasks_qty),
                        lookupAccounts.getCreditsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

                assertTrue(accounts.next());
                assertTrue(lookupAccounts.next());
                assertAccounts(accounts, lookupAccounts);

                assertEquals(BigInteger.valueOf(100 * tasks_qty), lookupAccounts.getDebitsPosted());
                assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testAccountTransfers() throws Throwable {

        try (var server = new Server()) {
            try (var client = new Client(clusterId, new String[] {server.getAddress()})) {

                // Creating the accounts.
                var createAccountErrors = client.createAccounts(accounts);
                assertTrue(createAccountErrors.getLength() == 0);

                // Creating a transfer.
                var transfers = new TransferBatch(10);
                for (int i = 0; i < 10; i++) {
                    transfers.add();
                    transfers.setId(i + 1);

                    // Swap the debit and credit accounts:
                    if (i % 2 == 0) {
                        transfers.setCreditAccountId(account1Id);
                        transfers.setDebitAccountId(account2Id);
                    } else {
                        transfers.setCreditAccountId(account2Id);
                        transfers.setDebitAccountId(account1Id);
                    }

                    transfers.setLedger(720);
                    transfers.setCode((short) 1);
                    transfers.setFlags(TransferFlags.NONE);
                    transfers.setAmount(100);
                }

                var createTransferErrors = client.createTransfers(transfers);
                assertTrue(createTransferErrors.getLength() == 0);
                {
                    // Querying transfers where:
                    // `debit_account_id=$account1Id OR credit_account_id=$account1Id
                    // ORDER BY timestamp ASC`.
                    var filter = new AccountTransfers();
                    filter.setAccountId(account1Id);
                    filter.setTimestamp(0);
                    filter.setLimit(8190);
                    filter.setDebits(true);
                    filter.setCredits(true);
                    filter.setReversed(false);
                    var account_transfers = client.getAccountTransfers(filter);
                    assertTrue(account_transfers.getLength() == 10);
                    long timestamp = 0;
                    while (account_transfers.next()) {
                        assertTrue(Long.compareUnsigned(account_transfers.getTimestamp(),
                                timestamp) > 0);
                        timestamp = account_transfers.getTimestamp();
                    }
                }

                {
                    // Querying transfers where:
                    // `debit_account_id=$account2Id OR credit_account_id=$account2Id
                    // ORDER BY timestamp DESC`.
                    var filter = new AccountTransfers();
                    filter.setAccountId(account2Id);
                    filter.setTimestamp(0);
                    filter.setLimit(8190);
                    filter.setDebits(true);
                    filter.setCredits(true);
                    filter.setReversed(true);
                    var account_transfers = client.getAccountTransfers(filter);

                    assertTrue(account_transfers.getLength() == 10);
                    long timestamp = Long.MIN_VALUE; // MIN_VALUE is the unsigned MAX_VALUE.
                    while (account_transfers.next()) {
                        assertTrue(Long.compareUnsigned(account_transfers.getTimestamp(),
                                timestamp) < 0);
                        timestamp = account_transfers.getTimestamp();
                    }
                }

                {
                    // Querying transfers where:
                    // `debit_account_id=$account1Id
                    // ORDER BY timestamp ASC`.
                    var filter = new AccountTransfers();
                    filter.setAccountId(account1Id);
                    filter.setTimestamp(0);
                    filter.setLimit(8190);
                    filter.setDebits(true);
                    filter.setCredits(false);
                    filter.setReversed(false);
                    var account_transfers = client.getAccountTransfers(filter);
                    assertTrue(account_transfers.getLength() == 5);
                    long timestamp = 0;
                    while (account_transfers.next()) {
                        assertTrue(Long.compareUnsigned(account_transfers.getTimestamp(),
                                timestamp) > 0);
                        timestamp = account_transfers.getTimestamp();
                    }
                }

                {
                    // Querying transfers where:
                    // `credit_account_id=$account2Id
                    // ORDER BY timestamp DESC`.
                    var filter = new AccountTransfers();
                    filter.setAccountId(account2Id);
                    filter.setTimestamp(0);
                    filter.setLimit(8190);
                    filter.setDebits(false);
                    filter.setCredits(true);
                    filter.setReversed(true);
                    var account_transfers = client.getAccountTransfers(filter);
                    assertTrue(account_transfers.getLength() == 5);
                    long timestamp = Long.MIN_VALUE; // MIN_VALUE is the unsigned MAX_VALUE.
                    while (account_transfers.next()) {
                        assertTrue(Long.compareUnsigned(account_transfers.getTimestamp(),
                                timestamp) < 0);
                        timestamp = account_transfers.getTimestamp();
                    }
                }

                {
                    // Querying transfers where:
                    // `debit_account_id=$account1Id OR credit_account_id=$account1Id
                    // ORDER BY timestamp ASC LIMIT 5`.
                    var filter = new AccountTransfers();
                    filter.setAccountId(account1Id);
                    filter.setTimestamp(0);
                    filter.setLimit(5);
                    filter.setDebits(true);
                    filter.setCredits(true);
                    filter.setReversed(false);

                    // First 5 items:
                    var account_transfers = client.getAccountTransfers(filter);
                    assertTrue(account_transfers.getLength() == 5);
                    long timestamp = 0;
                    while (account_transfers.next()) {
                        assertTrue(Long.compareUnsigned(account_transfers.getTimestamp(),
                                timestamp) > 0);
                        timestamp = account_transfers.getTimestamp();
                    }

                    // Next 5 items from this timestamp:
                    filter.setTimestamp(timestamp);
                    account_transfers = client.getAccountTransfers(filter);
                    assertTrue(account_transfers.getLength() == 5);
                    while (account_transfers.next()) {
                        assertTrue(Long.compareUnsigned(account_transfers.getTimestamp(),
                                timestamp) > 0);
                        timestamp = account_transfers.getTimestamp();
                    }

                    // No more pages after that:
                    filter.setTimestamp(timestamp);
                    account_transfers = client.getAccountTransfers(filter);
                    assertTrue(account_transfers.getLength() == 0);
                }

                {
                    // Querying transfers where:
                    // `debit_account_id=$account2Id OR credit_account_id=$account2Id
                    // ORDER BY timestamp DESC LIMIT 5`.
                    var filter = new AccountTransfers();
                    filter.setAccountId(account2Id);
                    filter.setTimestamp(0);
                    filter.setLimit(5);
                    filter.setDebits(true);
                    filter.setCredits(true);
                    filter.setReversed(true);

                    // First 5 items:
                    var account_transfers = client.getAccountTransfers(filter);
                    assertTrue(account_transfers.getLength() == 5);
                    long timestamp = Long.MIN_VALUE; // MIN_VALUE is the unsigned MAX_VALUE.
                    while (account_transfers.next()) {
                        assertTrue(Long.compareUnsigned(account_transfers.getTimestamp(),
                                timestamp) < 0);
                        timestamp = account_transfers.getTimestamp();
                    }

                    // Next 5 items from this timestamp:
                    filter.setTimestamp(timestamp);
                    account_transfers = client.getAccountTransfers(filter);
                    assertTrue(account_transfers.getLength() == 5);
                    while (account_transfers.next()) {
                        assertTrue(Long.compareUnsigned(account_transfers.getTimestamp(),
                                timestamp) < 0);
                        timestamp = account_transfers.getTimestamp();
                    }

                    // No more pages after that:
                    filter.setTimestamp(timestamp);
                    account_transfers = client.getAccountTransfers(filter);
                    assertTrue(account_transfers.getLength() == 0);
                }

                {
                    // Empty filter:
                    var filter = new AccountTransfers();
                    assertTrue(client.getAccountTransfers(filter).getLength() == 0);
                }

                {
                    // Invalid account:
                    var filter = new AccountTransfers();
                    filter.setAccountId(0);
                    filter.setTimestamp(0);
                    filter.setLimit(8190);
                    filter.setDebits(true);
                    filter.setCredits(true);
                    filter.setReversed(false);
                    assertTrue(client.getAccountTransfers(filter).getLength() == 0);
                }

                {
                    // Invalid timestamp:
                    var filter = new AccountTransfers();
                    filter.setAccountId(account2Id);
                    filter.setTimestamp(-1L); // -1L == ulong max value
                    filter.setLimit(8190);
                    filter.setDebits(true);
                    filter.setCredits(true);
                    filter.setReversed(false);
                    assertTrue(client.getAccountTransfers(filter).getLength() == 0);
                }

                {
                    // Zero limit:
                    var filter = new AccountTransfers();
                    filter.setAccountId(account2Id);
                    filter.setTimestamp(0);
                    filter.setLimit(0);
                    filter.setDebits(true);
                    filter.setCredits(true);
                    filter.setReversed(false);
                    assertTrue(client.getAccountTransfers(filter).getLength() == 0);
                }

                {
                    // Zero flags:
                    var filter = new AccountTransfers();
                    filter.setAccountId(account2Id);
                    filter.setTimestamp(0);
                    filter.setLimit(0);
                    filter.setDebits(false);
                    filter.setCredits(false);
                    filter.setReversed(false);
                    assertTrue(client.getAccountTransfers(filter).getLength() == 0);
                }

            } catch (Throwable any) {
                throw any;
            }

        } catch (Throwable any) {
            throw any;
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

    private static void assertTransfers(TransferBatch transfer1, TransferBatch transfer2) {
        assertArrayEquals(transfer1.getId(), transfer2.getId());
        assertArrayEquals(transfer1.getDebitAccountId(), transfer2.getDebitAccountId());
        assertArrayEquals(transfer1.getCreditAccountId(), transfer2.getCreditAccountId());
        assertEquals(transfer1.getAmount(), transfer2.getAmount());
        assertArrayEquals(transfer1.getPendingId(), transfer2.getPendingId());
        assertArrayEquals(transfer1.getUserData128(), transfer2.getUserData128());
        assertEquals(transfer1.getUserData64(), transfer2.getUserData64());
        assertEquals(transfer1.getUserData32(), transfer2.getUserData32());
        assertEquals(transfer1.getTimeout(), transfer2.getTimeout());
        assertEquals(transfer1.getLedger(), transfer2.getLedger());
        assertEquals(transfer1.getCode(), transfer2.getCode());
        assertEquals(transfer1.getFlags(), transfer2.getFlags());

    }

    private static class TransferTask extends Thread {
        public final Client client;
        public CreateTransferResultBatch result;
        public Throwable exception;
        private CountDownLatch enterBarrier;
        private CountDownLatch exitBarrier;

        public TransferTask(Client client, CountDownLatch enterBarrier,
                CountDownLatch exitBarrier) {
            this.client = client;
            this.result = null;
            this.exception = null;
            this.enterBarrier = enterBarrier;
            this.exitBarrier = exitBarrier;
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
                enterBarrier.countDown();
                enterBarrier.await();
                result = client.createTransfers(transfers);
            } catch (Throwable e) {
                exception = e;
            } finally {
                exitBarrier.countDown();
            }
        }
    }

    private static class Server implements AutoCloseable {

        public static final String TB_FILE = "./0_0.tigerbeetle.tests";
        public static final String TB_SERVER = "../../../zig-out/bin/tigerbeetle";

        private final Process process;
        private String address;

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

            var format = Runtime.getRuntime().exec(new String[] {exe, "format", "--cluster=0",
                    "--replica=0", "--replica-count=1", TB_FILE});
            if (format.waitFor() != 0) {
                var reader = new BufferedReader(new InputStreamReader(format.getErrorStream()));
                var error = reader.lines().collect(Collectors.joining(". "));
                throw new Exception("Format failed. " + error);
            }

            this.process = new ProcessBuilder().command(
                    new String[] {exe, "start", "--addresses=0", TB_FILE, "--cache-grid=512MB"})
                    .redirectError(Redirect.PIPE).start();

            if (process.waitFor(100, TimeUnit.MILLISECONDS))
                throw new Exception("Start server failed");

            final var addressLoggedEvent = new CountDownLatch(1);
            final var stderrLogged = new StringBuilder();
            final var stderrReader = new Thread(() -> {
                final var stderr = process.getErrorStream();
                final var listening = "listening on ";
                new BufferedReader(new InputStreamReader(stderr)).lines().forEach(line -> {
                    stderrLogged.append(line);
                    stderrLogged.append("\n");
                    final var found = line.indexOf(listening);
                    if (found != -1) {
                        address = line.substring(found + listening.length()).trim();
                        addressLoggedEvent.countDown();
                    }
                });
            });
            stderrReader.start();
            process.onExit().whenCompleteAsync((process, exception) -> {
                // Diagnose cases when the server crashes.
                System.out.println(stderrLogged);

                System.out.printf("TigerBeetle server exited with code %d\n\n",
                        process.exitValue());
            });

            if (!addressLoggedEvent.await(60L, TimeUnit.SECONDS)) {
                process.destroy();
                stderrReader.join();
                throw new Exception("failed to read the port, exitValue=" + process.exitValue()
                        + " stderr:\n" + stderrLogged.toString());
            }
        }

        public String getAddress() {
            return address;
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
