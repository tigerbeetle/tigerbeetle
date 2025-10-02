package com.tigerbeetle.samples;

import java.sql.ResultSet;

// section:imports
import com.tigerbeetle.*;

public final class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Import ok!");
        // endsection:imports

        // section:client
        String replicaAddress = System.getenv("TB_ADDRESS");
        byte[] clusterID = UInt128.asBytes(0);
        String[] replicaAddresses = new String[] {replicaAddress == null ? "3000" : replicaAddress};
        try (var client = new Client(clusterID, replicaAddresses)) {
            // Use client
        }
        // endsection:client

        try (var client = new Client(clusterID, replicaAddresses)) {

            try {
                // section:create-accounts
                AccountBatch accounts = new AccountBatch(1);
                accounts.add();
                accounts.setId(UInt128.id()); // TigerBeetle time-based ID.
                accounts.setUserData128(0, 0);
                accounts.setUserData64(0);
                accounts.setUserData32(0);
                accounts.setLedger(1);
                accounts.setCode(718);
                accounts.setFlags(AccountFlags.NONE);
                accounts.setTimestamp(0);

                CreateAccountResultBatch accountErrors = client.createAccounts(accounts);
                // Error handling omitted.
                // endsection:create-accounts
            } catch (Throwable any) {}

            try {
                // section:account-flags
                AccountBatch accounts = new AccountBatch(2);

                accounts.add();
                accounts.setId(100);
                accounts.setLedger(1);
                accounts.setCode(718);
                accounts.setFlags(AccountFlags.LINKED | AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS);

                accounts.add();
                accounts.setId(101);
                accounts.setLedger(1);
                accounts.setCode(718);
                accounts.setFlags(AccountFlags.HISTORY);

                CreateAccountResultBatch accountErrors = client.createAccounts(accounts);
                // Error handling omitted.
                // endsection:account-flags
            } catch (Throwable any) {}

            try {
                // section:create-accounts-errors
                AccountBatch accounts = new AccountBatch(3);

                accounts.add();
                accounts.setId(102);
                accounts.setLedger(1);
                accounts.setCode(718);
                accounts.setFlags(AccountFlags.NONE);

                accounts.add();
                accounts.setId(103);
                accounts.setLedger(1);
                accounts.setCode(718);
                accounts.setFlags(AccountFlags.NONE);

                accounts.add();
                accounts.setId(104);
                accounts.setLedger(1);
                accounts.setCode(718);
                accounts.setFlags(AccountFlags.NONE);

                CreateAccountResultBatch accountErrors = client.createAccounts(accounts);
                while (accountErrors.next()) {
                    switch (accountErrors.getResult()) {
                        case Exists:
                            System.err.printf("Batch account at %d already exists.\n",
                                    accountErrors.getIndex());
                            break;

                        default:
                            System.err.printf("Batch account at %d failed to create %s.\n",
                                    accountErrors.getIndex(), accountErrors.getResult());
                            break;
                    }
                }
                // endsection:create-accounts-errors
            } catch (Throwable any) {}

            try {
                // section:lookup-accounts
                IdBatch ids = new IdBatch(2);
                ids.add(100);
                ids.add(101);

                AccountBatch accounts = client.lookupAccounts(ids);
                // endsection:lookup-accounts
            } catch (Throwable any) {}

            try {
                // section:create-transfers
                TransferBatch transfers = new TransferBatch(1);

                transfers.add();
                transfers.setId(UInt128.id());
                transfers.setDebitAccountId(102);
                transfers.setCreditAccountId(103);
                transfers.setAmount(10);
                transfers.setUserData128(0, 0);
                transfers.setUserData64(0);
                transfers.setUserData32(0);
                transfers.setTimeout(0);
                transfers.setLedger(1);
                transfers.setCode(1);
                transfers.setFlags(TransferFlags.NONE);
                transfers.setTimeout(0);

                CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
                // Error handling omitted.
                // endsection:create-transfers
            } catch (Throwable any) {}

            try {
                // section:create-transfers-errors
                TransferBatch transfers = new TransferBatch(3);

                transfers.add();
                transfers.setId(1);
                transfers.setDebitAccountId(102);
                transfers.setCreditAccountId(103);
                transfers.setAmount(10);
                transfers.setLedger(1);
                transfers.setCode(1);

                transfers.add();
                transfers.setId(2);
                transfers.setDebitAccountId(102);
                transfers.setCreditAccountId(103);
                transfers.setAmount(10);
                transfers.setLedger(1);
                transfers.setCode(1);

                transfers.add();
                transfers.setId(3);
                transfers.setDebitAccountId(102);
                transfers.setCreditAccountId(103);
                transfers.setAmount(10);
                transfers.setLedger(1);
                transfers.setCode(1);

                CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
                while (transferErrors.next()) {
                    switch (transferErrors.getResult()) {
                        case ExceedsCredits:
                            System.err.printf("Batch transfer at %d already exists.\n",
                                    transferErrors.getIndex());
                            break;

                        default:
                            System.err.printf("Batch transfer at %d failed to create: %s\n",
                                    transferErrors.getIndex(), transferErrors.getResult());
                            break;
                    }
                }
                // endsection:create-transfers-errors
            } catch (Throwable any) {}

            try {
                // section:no-batch
                ResultSet dataSource = null; /* Loaded from an external source. */;
                while(dataSource.next()) {
                    TransferBatch batch = new TransferBatch(1);

                    batch.add();
                    batch.setId(dataSource.getBytes("id"));
                    batch.setDebitAccountId(dataSource.getBytes("debit_account_id"));
                    batch.setCreditAccountId(dataSource.getBytes("credit_account_id"));
                    batch.setAmount(dataSource.getBigDecimal("amount").toBigInteger());
                    batch.setLedger(dataSource.getInt("ledger"));
                    batch.setCode(dataSource.getInt("code"));

                    CreateTransferResultBatch transferErrors = client.createTransfers(batch);
                    // Error handling omitted.
                }
                // endsection:no-batch
            } catch (Throwable any) {}

            try {
                // section:batch
                ResultSet dataSource = null; /* Loaded from an external source. */;

                var BATCH_SIZE = 8189;
                TransferBatch batch = new TransferBatch(BATCH_SIZE);
                while(dataSource.next()) {
                    batch.add();
                    batch.setId(dataSource.getBytes("id"));
                    batch.setDebitAccountId(dataSource.getBytes("debit_account_id"));
                    batch.setCreditAccountId(dataSource.getBytes("credit_account_id"));
                    batch.setAmount(dataSource.getBigDecimal("amount").toBigInteger());
                    batch.setLedger(dataSource.getInt("ledger"));
                    batch.setCode(dataSource.getInt("code"));

                    if (batch.getLength() == BATCH_SIZE) {
                        CreateTransferResultBatch transferErrors = client.createTransfers(batch);
                        // Error handling omitted.

                        // Reset the batch for the next iteration.
                        batch.beforeFirst();
                    }
                }

                if (batch.getLength() > 0) {
                    // Send the remaining items.
                    CreateTransferResultBatch transferErrors = client.createTransfers(batch);
                    // Error handling omitted.
                }

                // endsection:batch
            } catch (Throwable any) {}

            try {
                // section:transfer-flags-link
                TransferBatch transfers = new TransferBatch(2);

                // First transfer
                transfers.add();
                transfers.setId(4);
                transfers.setDebitAccountId(102);
                transfers.setCreditAccountId(103);
                transfers.setAmount(10);
                transfers.setLedger(1);
                transfers.setCode(1);
                transfers.setFlags(TransferFlags.LINKED);

                transfers.add();
                transfers.setId(5);
                transfers.setDebitAccountId(102);
                transfers.setCreditAccountId(103);
                transfers.setAmount(10);
                transfers.setLedger(1);
                transfers.setCode(1);
                transfers.setFlags(TransferFlags.NONE);

                CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
                // Error handling omitted.
                // endsection:transfer-flags-link
            } catch (Throwable any) {}

            try {
                // section:transfer-flags-post
                TransferBatch transfers = new TransferBatch(1);

                transfers.add();
                transfers.setId(6);
                transfers.setDebitAccountId(102);
                transfers.setCreditAccountId(103);
                transfers.setAmount(10);
                transfers.setLedger(1);
                transfers.setCode(1);
                transfers.setFlags(TransferFlags.PENDING);

                CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
                // Error handling omitted.

                transfers = new TransferBatch(1);

                transfers.add();
                transfers.setId(7);
                transfers.setAmount(TransferBatch.AMOUNT_MAX);
                transfers.setPendingId(6);
                transfers.setFlags(TransferFlags.POST_PENDING_TRANSFER);

                transferErrors = client.createTransfers(transfers);
                // Error handling omitted.
                // endsection:transfer-flags-post
            } catch (Throwable any) {}

            try {
                // section:transfer-flags-void
                TransferBatch transfers = new TransferBatch(1);

                transfers.add();
                transfers.setId(8);
                transfers.setDebitAccountId(102);
                transfers.setCreditAccountId(103);
                transfers.setAmount(10);
                transfers.setLedger(1);
                transfers.setCode(1);
                transfers.setFlags(TransferFlags.PENDING);

                CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
                // Error handling omitted.

                transfers = new TransferBatch(1);

                transfers.add();
                transfers.setId(9);
                transfers.setAmount(0);
                transfers.setPendingId(8);
                transfers.setFlags(TransferFlags.VOID_PENDING_TRANSFER);

                transferErrors = client.createTransfers(transfers);
                // Error handling omitted.
                // endsection:transfer-flags-void
            } catch (Throwable any) {}

            try {
                // section:lookup-transfers
                IdBatch ids = new IdBatch(2);
                ids.add(1);
                ids.add(2);

                TransferBatch transfers = client.lookupTransfers(ids);
                // endsection:lookup-transfers
            } catch (Throwable any) {}

            try {
                // section:get-account-transfers
                AccountFilter filter = new AccountFilter();
                filter.setAccountId(2);
                filter.setUserData128(0); // No filter by UserData.
                filter.setUserData64(0);
                filter.setUserData32(0);
                filter.setCode(0); // No filter by Code.
                filter.setTimestampMin(0); // No filter by Timestamp.
                filter.setTimestampMax(0); // No filter by Timestamp.
                filter.setLimit(10); // Limit to ten transfers at most.
                filter.setDebits(true); // Include transfer from the debit side.
                filter.setCredits(true); // Include transfer from the credit side.
                filter.setReversed(true); // Sort by timestamp in reverse-chronological order.

                TransferBatch transfers = client.getAccountTransfers(filter);
                // endsection:get-account-transfers
            } catch (Throwable any) {}

            try {
                // section:get-account-balances
                AccountFilter filter = new AccountFilter();
                filter.setAccountId(2);
                filter.setUserData128(0); // No filter by UserData.
                filter.setUserData64(0);
                filter.setUserData32(0);
                filter.setCode(0); // No filter by Code.
                filter.setTimestampMin(0); // No filter by Timestamp.
                filter.setTimestampMax(0); // No filter by Timestamp.
                filter.setLimit(10); // Limit to ten balances at most.
                filter.setDebits(true); // Include transfer from the debit side.
                filter.setCredits(true); // Include transfer from the credit side.
                filter.setReversed(true); // Sort by timestamp in reverse-chronological order.

                AccountBalanceBatch account_balances = client.getAccountBalances(filter);
                // endsection:get-account-balances
            } catch (Throwable any) {}

            try {
                // section:query-accounts
                QueryFilter filter = new QueryFilter();
                filter.setUserData128(1000); // Filter by UserData.
                filter.setUserData64(100);
                filter.setUserData32(10);
                filter.setCode(1); // Filter by Code.
                filter.setLedger(0); // No filter by Ledger.
                filter.setTimestampMin(0); // No filter by Timestamp.
                filter.setTimestampMax(0); // No filter by Timestamp.
                filter.setLimit(10); // Limit to ten accounts at most.
                filter.setReversed(true); // Sort by timestamp in reverse-chronological order.

                AccountBatch accounts = client.queryAccounts(filter);
                // endsection:query-accounts
            } catch (Throwable any) {}

            try {
                // section:query-transfers
                QueryFilter filter = new QueryFilter();
                filter.setUserData128(1000); // Filter by UserData.
                filter.setUserData64(100);
                filter.setUserData32(10);
                filter.setCode(1); // Filter by Code.
                filter.setLedger(0); // No filter by Ledger.
                filter.setTimestampMin(0); // No filter by Timestamp.
                filter.setTimestampMax(0); // No filter by Timestamp.
                filter.setLimit(10); // Limit to ten transfers at most.
                filter.setReversed(true); // Sort by timestamp in reverse-chronological order.

                TransferBatch transfers = client.queryTransfers(filter);
                // endsection:query-transfers
            } catch (Throwable any) {}

            try {
                // section:linked-events
                TransferBatch transfers = new TransferBatch(10);

                // An individual transfer (successful):
                transfers.add();
                transfers.setId(1);
                // ... rest of transfer ...
                transfers.setFlags(TransferFlags.NONE);

                // A chain of 4 transfers (the last transfer in the chain closes the chain with
                // linked=false):
                transfers.add();
                transfers.setId(2); // Commit/rollback.
                // ... rest of transfer ...
                transfers.setFlags(TransferFlags.LINKED);
                transfers.add();
                transfers.setId(3); // Commit/rollback.
                // ... rest of transfer ...
                transfers.setFlags(TransferFlags.LINKED);
                transfers.add();
                transfers.setId(2); // Fail with exists
                // ... rest of transfer ...
                transfers.setFlags(TransferFlags.LINKED);
                transfers.add();
                transfers.setId(4); // Fail without committing
                // ... rest of transfer ...
                transfers.setFlags(TransferFlags.NONE);

                // An individual transfer (successful):
                // This should not see any effect from the failed chain above.
                transfers.add();
                transfers.setId(2);
                // ... rest of transfer ...
                transfers.setFlags(TransferFlags.NONE);

                // A chain of 2 transfers (the first transfer fails the chain):
                transfers.add();
                transfers.setId(2);
                // ... rest of transfer ...
                transfers.setFlags(TransferFlags.LINKED);
                transfers.add();
                transfers.setId(3);
                // ... rest of transfer ...
                transfers.setFlags(TransferFlags.NONE);
                // A chain of 2 transfers (successful):
                transfers.add();
                transfers.setId(3);
                // ... rest of transfer ...
                transfers.setFlags(TransferFlags.LINKED);
                transfers.add();
                transfers.setId(4);
                // ... rest of transfer ...
                transfers.setFlags(TransferFlags.NONE);

                CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
                // Error handling omitted.
                // endsection:linked-events
            } catch (Throwable any) {}

            try {
                // section:imported-events
                // External source of time
                long historicalTimestamp = 0L;
                ResultSet historicalAccounts = null; // Loaded from an external source;
                ResultSet historicalTransfers = null ; // Loaded from an external source.

                var BATCH_SIZE = 8189;

                // First, load and import all accounts with their timestamps from the historical source.
                AccountBatch accounts = new AccountBatch(BATCH_SIZE);
                while (historicalAccounts.next()) {
                    // Set a unique and strictly increasing timestamp.
                    historicalTimestamp += 1;

                    accounts.add();
                    accounts.setId(historicalAccounts.getBytes("id"));
                    accounts.setLedger(historicalAccounts.getInt("ledger"));
                    accounts.setCode(historicalAccounts.getInt("code"));
                    accounts.setTimestamp(historicalTimestamp);

                    // Set the account as `imported`.
                    // To ensure atomicity, the entire batch (except the last event in the chain)
                    // must be `linked`.
                    if (accounts.getLength() < BATCH_SIZE) {
                        accounts.setFlags(AccountFlags.IMPORTED | AccountFlags.LINKED);
                    } else {
                        accounts.setFlags(AccountFlags.IMPORTED);

                        CreateAccountResultBatch accountsErrors = client.createAccounts(accounts);
                        // Error handling omitted.

                        // Reset the batch for the next iteration.
                        accounts.beforeFirst();
                    }
                }

                if (accounts.getLength() > 0) {
                    // Send the remaining items.
                    CreateAccountResultBatch accountsErrors = client.createAccounts(accounts);
                    // Error handling omitted.
                }

                // Then, load and import all transfers with their timestamps from the historical source.
                TransferBatch transfers = new TransferBatch(BATCH_SIZE);
                while (historicalTransfers.next()) {
                    // Set a unique and strictly increasing timestamp.
                    historicalTimestamp += 1;

                    transfers.add();
                    transfers.setId(historicalTransfers.getBytes("id"));
                    transfers.setDebitAccountId(historicalTransfers.getBytes("debit_account_id"));
                    transfers.setCreditAccountId(historicalTransfers.getBytes("credit_account_id"));
                    transfers.setAmount(historicalTransfers.getBigDecimal("amount").toBigInteger());
                    transfers.setLedger(historicalTransfers.getInt("ledger"));
                    transfers.setCode(historicalTransfers.getInt("code"));
                    transfers.setTimestamp(historicalTimestamp);

                    // Set the transfer as `imported`.
                    // To ensure atomicity, the entire batch (except the last event in the chain)
                    // must be `linked`.
                    if (transfers.getLength() < BATCH_SIZE) {
                        transfers.setFlags(TransferFlags.IMPORTED | TransferFlags.LINKED);
                    } else {
                        transfers.setFlags(TransferFlags.IMPORTED);

                        CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
                        // Error handling omitted.

                        // Reset the batch for the next iteration.
                        transfers.beforeFirst();
                    }
                }

                if (transfers.getLength() > 0) {
                    // Send the remaining items.
                    CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
                    // Error handling omitted.
                }

                // Since it is a linked chain, in case of any error the entire batch is rolled back and can be retried
                // with the same historical timestamps without regressing the cluster timestamp.
                // endsection:imported-events
            } catch (Throwable any) {}
        }
        // section:imports
    }
}
// endsection:imports
