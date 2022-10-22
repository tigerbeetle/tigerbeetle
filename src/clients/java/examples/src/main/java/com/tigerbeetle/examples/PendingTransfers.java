package com.tigerbeetle.examples;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import com.tigerbeetle.*;

/**
 * Demonstrates how pending transfers work: pending credits/debits, timeouts, and posting/voiding a
 * transfer.
 */
public final class PendingTransfers {

    public static void main(String[] args) {

        final var cluster = Util.getCluster(args);
        final var addresses = Util.getAddresses(args);

        System.out.println("TigerBeetle PendingTransfers example");

        try (var client = new Client(cluster, addresses)) {

            /*
             * ################### Step 1 ###################
             */
            System.out.printf("Creating accounts ...%n");

            // Creating a batch of accounts.
            final var accounts = new AccountBatch(2);

            // Adding the first account.
            accounts.add();

            final var bankReserve = UInt128.asBytes(UUID.randomUUID());
            accounts.setId(bankReserve);
            accounts.setUserData(null);
            accounts.setCode(AccountCodes.BANK_RESERVE.Code);
            accounts.setLedger(Ledgers.GBP.Code);

            // Adding another account
            accounts.add();

            final var customerId = UInt128.asBytes(UUID.randomUUID());
            accounts.setId(customerId);
            accounts.setUserData(null);
            accounts.setCode(AccountCodes.CUSTOMER.Code);
            accounts.setLedger(Ledgers.GBP.Code);
            accounts.setFlags(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS);

            // Submitting the accounts for creation.
            final var accountErrors = client.createAccounts(accounts);

            // Checking for errors during the creation.
            if (accountErrors.getLength() > 0) {

                while (accountErrors.next()) {

                    final var errorIndex = accountErrors.getIndex();
                    accounts.setPosition(errorIndex);

                    System.err.printf("Error creating account %s: %s.%n",
                            UInt128.asBigInteger(accounts.getId()), accountErrors.getResult());
                }

                return;
            }

            /*
             * ################### Step 2 ###################
             */
            System.out.printf("Creating a pending transfer ...%n");

            // Creating a batch of transfers.
            // See: https://docs.tigerbeetle.com/reference/transfers/#flagspending
            var transfers = new TransferBatch(1);

            // Example: The customer receives a GBP 500.00 deposit.
            transfers.add();
            final var depositId = UInt128.asBytes(UUID.randomUUID());
            transfers.setId(depositId);
            transfers.setDebitAccountId(bankReserve);
            transfers.setCreditAccountId(customerId);
            transfers.setCode(TransferCodes.DEPOSIT.Code);
            transfers.setLedger(Ledgers.GBP.Code);
            transfers.setAmount(50000L);
            transfers.setFlags(TransferFlags.PENDING); // This transfer is pending.
            transfers.setTimeout(TimeUnit.MINUTES.toNanos(5)); // And will expire in 5 minutes.

            // Submitting the transfer.
            var transferErrors = client.createTransfers(transfers);

            // Checking for any errors during the transfer.
            if (transferErrors.next()) {
                System.err.printf("Error creating the transfer: %s.%n", transferErrors.getResult());
                return;
            }

            /*
             * ################### Step 3 ###################
             */
            System.out.printf("Checking the account's balance after the pending transfer:%n");

            // Creating a batch with the account's ids.
            var ids = new IdBatch(2);
            ids.add(bankReserve);
            ids.add(customerId);

            // Looking up the accounts.
            var balances = client.lookupAccounts(ids);
            Util.printAccounts(balances);

            /*
             * ################### Step 4 ###################
             */
            System.out.printf("Posting the the pending transfer ...%n");

            // Creating a batch of transfers.
            transfers = new TransferBatch(1);

            // Example: Confirming the pending GBP 500.00 deposit.
            transfers.add();
            final var postingDepositId = UInt128.asBytes(UUID.randomUUID());
            transfers.setId(postingDepositId);
            transfers.setDebitAccountId(bankReserve);
            transfers.setCreditAccountId(customerId);
            transfers.setCode(TransferCodes.DEPOSIT.Code);
            transfers.setLedger(Ledgers.GBP.Code);
            transfers.setAmount(50000L);
            transfers.setFlags(TransferFlags.POST_PENDING_TRANSFER); // This transfer confirms the
                                                                     // pending one.
            transfers.setPendingId(depositId);

            // Submitting the transfers.
            transferErrors = client.createTransfers(transfers);

            if (transferErrors.getLength() > 0) {

                while (transferErrors.next()) {
                    final var errorIndex = transferErrors.getIndex();
                    transfers.setPosition(errorIndex);
                    System.err.printf("Error creating the transfers %s: %s.%n",
                            UInt128.asBigInteger(transfers.getId()), transferErrors.getResult());
                }

                return;
            }

            /*
             * ################### Step 5 ###################
             */
            System.out
                    .printf("Checking the account's balance after posting the pending transfer:%n");

            // Creating a batch with the account's ids.
            ids = new IdBatch(2);
            ids.add(bankReserve);
            ids.add(customerId);

            // Looking up the accounts.
            balances = client.lookupAccounts(ids);
            Util.printAccounts(balances);


            /*
             * ################### Step 6 ###################
             */
            System.out.printf("Creating another pending transfer ...%n");

            // Creating a batch of transfers.
            // See: https://docs.tigerbeetle.com/reference/transfers/#flagspending
            transfers = new TransferBatch(1);

            // Example: The customer withdraws GBP 100.00 from their account.
            transfers.add();
            final var withdrawId = UInt128.asBytes(UUID.randomUUID());
            transfers.setId(withdrawId);
            transfers.setDebitAccountId(customerId);
            transfers.setCreditAccountId(bankReserve);
            transfers.setCode(TransferCodes.WITHDRAW.Code);
            transfers.setLedger(Ledgers.GBP.Code);
            transfers.setAmount(10000L);
            transfers.setFlags(TransferFlags.PENDING); // This transfer is pending
            transfers.setTimeout(TimeUnit.MINUTES.toNanos(5)); // And will expire in 5 minutes

            // Submitting the transfer
            transferErrors = client.createTransfers(transfers);

            // Checking for any errors during the transfer.
            if (transferErrors.next()) {
                System.err.printf("Error creating the transfer: %s.%n", transferErrors.getResult());
                return;
            }

            /*
             * ################### Step 7 ###################
             */
            System.out.printf("Checking the account's balance after the pending transfer:%n");

            // Creating a batch with the account's ids.
            ids = new IdBatch(2);
            ids.add(bankReserve);
            ids.add(customerId);

            // Looking up the accounts.
            balances = client.lookupAccounts(ids);
            Util.printAccounts(balances);

            /*
             * ################### Step 8 ###################
             */
            System.out.printf("Voiding the pending transfers ...%n");

            // Creating a batch of transfers.
            // See https://docs.tigerbeetle.com/reference/transfers/#flagsvoid_pending_transfer
            transfers = new TransferBatch(1);

            // Example: Confirming the pending GBP 500.00 deposit.
            transfers.add();
            final var postingWithdrawId = UInt128.asBytes(UUID.randomUUID());
            transfers.setId(postingWithdrawId);
            transfers.setDebitAccountId(customerId);
            transfers.setCreditAccountId(bankReserve);
            transfers.setCode(TransferCodes.WITHDRAW.Code);
            transfers.setLedger(Ledgers.GBP.Code);
            transfers.setAmount(10000L);
            transfers.setFlags(TransferFlags.VOID_PENDING_TRANSFER); // This transfer voids the
                                                                     // pending one.
            transfers.setPendingId(withdrawId);

            // Submitting the transfers
            transferErrors = client.createTransfers(transfers);

            if (transferErrors.getLength() > 0) {

                while (transferErrors.next()) {
                    final var errorIndex = transferErrors.getIndex();
                    transfers.setPosition(errorIndex);
                    System.err.printf("Error creating the transfers %s: %s.%n",
                            UInt128.asBigInteger(transfers.getId()), transferErrors.getResult());
                }

                return;
            }

            /*
             * ################### Step 9 ###################
             */
            System.out.printf(
                    "Checking the account's balance after voiding the pending transfers:%n");

            // Creating a batch with the account's ids.
            ids = new IdBatch(2);
            ids.add(bankReserve);
            ids.add(customerId);

            // Looking up the accounts.
            balances = client.lookupAccounts(ids);
            Util.printAccounts(balances);


        } catch (Exception exception) {
            System.err.printf("Unexpected exception %s%n", exception);
        }
    }
}
